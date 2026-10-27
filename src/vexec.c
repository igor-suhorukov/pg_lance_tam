/*-------------------------------------------------------------------------
 *
 * vexec.c
 *	  pg_vexec's batch source and batch sink for lance tables
 *	  (docs/design.md §2.13; the contracts are pg_accel's vexec_source.h and
 *	  vexec_sink.h, copied into include/vexec/).  Neither side links the
 *	  other: without vexec the registrations are unused entries of its
 *	  registries, and the extension builds and works without it.
 *
 *	  The source.  vexec begins the scan itself, through table_beginscan(),
 *	  and hands it to begin(): the batches come from scan.c's reader of the
 *	  scan's snapshot, the columns vexec needs read and its pruning quals
 *	  given to Lance as a filter (filter.c).  Lance's Arrow columns are
 *	  handed in their own layouts, without copies where vexec reads them as
 *	  they lie:
 *
 *		a Lance column			vexec's layout				copies
 *		int16, int32, int64,	FIXED, the type's width		none
 *		 float32, float64
 *		date32, timestamp[us]	FIXED, arrow_values			none
 *		time64[us]				FIXED						none
 *		bool					BIT_BOOL					none, or a shift
 *		decimal128				SCALED, width 16			none
 *		utf8, binary			OFFSETS						none
 *		fixed_size_binary(16)	FIXED, width 16 (uuid)		none
 *		vectors, lists, text	DATUM						every value
 *		 forms of other types
 *
 *	  with the validity bitmaps as they are where a batch starts on a word,
 *	  and the row ids as TIDs.  A batch's arrays stay until vexec releases
 *	  it, when it retained it, or until the next batch.
 *
 *	  The sink.  VecInsert's batches are written as rows of the statement's
 *	  command, each given its row id and TID, as tuple_insert gives them,
 *	  through xact.c's batch builder.
 *
 *	  Both are registered while the postmaster preloads the library.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "varatt.h"

#include "vexec/vexec_sink.h"
#include "vexec/vexec_source.h"

#include "lance_tam.h"

/* An Arrow batch read, kept while a batch handed to vexec points into it */
typedef struct Holder
{
	LtBatch		batch;
	int			refs;
} Holder;

/* One batch handed to vexec: its arrays, and the Arrow batch they point into */
typedef struct SrcOut
{
	MemoryContext cxt;
	VexecColumn *columns;
	ItemPointerData *tids;
	Holder	   *holder;
	bool		retained;
} SrcOut;

typedef struct SrcState
{
	TableScanDesc scan;
	Relation	rel;
	MemoryContext cxt;
	LtReadSpec	spec;
	int			ncols;
	AttrNumber *attnums;
	bool		want_tids;
	int			max_rows;
	LtReader   *reader;
	Holder	   *current;		/* the Arrow batch being handed, a chunk at a
								 * time */
	int64		pos;			/* its next row */
	SrcOut	   *last;			/* the batch handed last */
	List	   *outs;			/* every out made, for the end */
	bool		done;
} SrcState;

static void
holder_unref(Holder *h)
{
	if (--h->refs == 0)
	{
		lt_batch_release(&h->batch);
		pfree(h);
	}
}

static void
out_reset(SrcOut *out)
{
	if (out->holder)
		holder_unref(out->holder);
	out->holder = NULL;
	out->columns = NULL;
	out->tids = NULL;
	MemoryContextReset(out->cxt);
}

static bool
src_supports(Relation rel, AttrNumber attnum)
{
	if (attnum == SelfItemPointerAttributeNumber)
		return true;
	return attnum >= 1 && attnum <= RelationGetNumberOfAttributes(rel) &&
		!TupleDescAttr(RelationGetDescr(rel), attnum - 1)->attisdropped;
}

static void *
src_begin(TableScanDesc scan, const VexecSourceSpec *spec)
{
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext, "lance_tam vexec source",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContext old = MemoryContextSwitchTo(cxt);
	SrcState   *st = palloc0(sizeof(SrcState));
	LtTable    *t;

	st->scan = scan;
	st->rel = scan->rs_rd;
	st->cxt = cxt;
	st->ncols = spec->ncolumns;
	st->attnums = palloc(sizeof(AttrNumber) * Max(spec->ncolumns, 1));
	memcpy(st->attnums, spec->attnums, sizeof(AttrNumber) * spec->ncolumns);
	st->want_tids = (spec->flags & VEXEC_SRC_TIDS) != 0;
	st->max_rows = spec->max_rows > 0 ? spec->max_rows : lt_batch_rows;

	t = lt_table(st->rel);
	memset(&st->spec, 0, sizeof(st->spec));
	st->spec.limit = -1;
	st->spec.with_rowid = true;
	for (int i = 0; i < spec->ncolumns; i++)
		st->spec.attnums = lappend_int(st->spec.attnums, spec->attnums[i]);
	if (st->spec.attnums == NIL)
	{
		/* no column: vexec counts rows; read the narrowest */
		st->spec.attnums = list_make1_int(1);
	}
	st->spec.filter = lt_filter_from_quals(st->rel, t, spec->quals, NULL);
	MemoryContextSwitchTo(old);
	return st;
}

/* validity bits from row "first" of an array, as uint64 words from bit 0 */
static const uint64 *
validity_words(SrcOut *out, const struct ArrowArray *a, int64 first, int nrows)
{
	const uint8 *bits;
	int64		start;
	uint64	   *words;

	if (a->null_count == 0 || a->n_buffers == 0 || a->buffers[0] == NULL)
		return NULL;
	bits = (const uint8 *) a->buffers[0];
	start = a->offset + first;
	if (start % 64 == 0 && ((uintptr_t) bits % 8) == 0)
		return ((const uint64 *) bits) + start / 64;
	words = MemoryContextAllocZero(out->cxt, sizeof(uint64) * ((nrows + 63) / 64 + 1));
	for (int i = 0; i < nrows; i++)
		if ((bits[(start + i) >> 3] >> ((start + i) & 7)) & 1)
			words[i >> 6] |= UINT64CONST(1) << (i & 63);
	return words;
}

static const void *
bool_values(SrcOut *out, const struct ArrowArray *a, int64 first, int nrows)
{
	const uint8 *bits = (const uint8 *) a->buffers[1];
	int64		start = a->offset + first;
	uint64	   *words;

	if (start % 64 == 0 && ((uintptr_t) bits % 8) == 0)
		return ((const uint64 *) bits) + start / 64;
	words = MemoryContextAllocZero(out->cxt, sizeof(uint64) * ((nrows + 63) / 64 + 1));
	for (int i = 0; i < nrows; i++)
		if ((bits[(start + i) >> 3] >> ((start + i) & 7)) & 1)
			words[i >> 6] |= UINT64CONST(1) << (i & 63);
	return words;
}

static void
fill_column(SrcOut *out, LtBatch *batch, int ci, int64 first, int nrows, VexecColumn *vc)
{
	const struct ArrowArray *a = batch->array.children[ci];
	LtColumn   *col = batch->cols[ci];
	int64		start = a->offset + first;

	memset(vc, 0, sizeof(VexecColumn));
	vc->encoding = VEXEC_FLAT;
	vc->validity = validity_words(out, a, first, nrows);
	switch (col->kind)
	{
		case LT_INT2:
		case LT_INT4:
		case LT_INT8:
		case LT_FLOAT4:
		case LT_FLOAT8:
		case LT_DATE:
		case LT_TIME:
		case LT_TIMESTAMP:
		case LT_TIMESTAMPTZ:
		case LT_UUID:
			{
				int			w;

				switch (col->kind)
				{
					case LT_INT2:
						w = 2;
						break;
					case LT_INT4:
					case LT_FLOAT4:
					case LT_DATE:
						w = 4;
						break;
					case LT_UUID:
						w = 16;
						break;
					default:
						w = 8;
						break;
				}
				vc->layout = VEXEC_FIXED;
				vc->width = w;
				vc->stride = w;
				vc->arrow_values = col->kind == LT_DATE || col->kind == LT_TIMESTAMP ||
					col->kind == LT_TIMESTAMPTZ;
				vc->values = ((const char *) a->buffers[1]) + (size_t) w * start;
				return;
			}
		case LT_BOOL:
			vc->layout = VEXEC_BIT_BOOL;
			vc->values = bool_values(out, a, first, nrows);
			return;
		case LT_NUMERIC:
			vc->layout = VEXEC_SCALED;
			vc->width = 16;
			vc->stride = 16;
			vc->scale = (int16) col->scale;
			vc->values = ((const char *) a->buffers[1]) + (size_t) 16 * start;
			return;
		case LT_TEXT:
		case LT_BYTEA:
			if (col->kind == LT_BYTEA || GetDatabaseEncoding() == PG_UTF8 ||
				GetDatabaseEncoding() == PG_SQL_ASCII)
			{
				const int32 *off = (const int32 *) a->buffers[1];
				const void **bufs = MemoryContextAlloc(out->cxt, sizeof(void *));
				int64	   *sizes = MemoryContextAlloc(out->cxt, sizeof(int64));

				bufs[0] = a->buffers[2];
				sizes[0] = off[a->offset + a->length];
				vc->layout = VEXEC_OFFSETS;
				vc->values = off + start;
				vc->buffers = bufs;
				vc->buffer_sizes = sizes;
				vc->nbuffers = 1;
				return;
			}
			break;
		default:
			break;
	}
	{
			{
				/* a Datum a row, made in the batch's memory */
				Datum	   *datums = MemoryContextAllocZero(out->cxt, sizeof(Datum) * Max(nrows, 1));
				MemoryContext old = MemoryContextSwitchTo(out->cxt);

				for (int i = 0; i < nrows; i++)
				{
					bool		isnull;

					datums[i] = lt_batch_datum(batch, ci, first + i, &isnull);
				}
				MemoryContextSwitchTo(old);
				vc->layout = VEXEC_DATUM;
				vc->values = datums;
				return;
			}
	}
}

static bool
src_next(void *arg, VexecSourceBatch *result)
{
	SrcState   *st = (SrcState *) arg;
	SrcOut	   *out;
	LtBatch    *batch;
	int			nrows;

	CHECK_FOR_INTERRUPTS();
	/* the batch handed last is vexec's no more, unless it retained it */
	out = st->last;
	if (out && !out->retained)
		out_reset(out);
	else
		out = NULL;

	while (st->current == NULL || st->pos >= st->current->batch.nrows)
	{
		if (st->current)
		{
			holder_unref(st->current);
			st->current = NULL;
		}
		if (st->done)
			return false;
		if (st->reader == NULL)
		{
			MemoryContext old = MemoryContextSwitchTo(st->cxt);

			st->reader = lt_scan_take_reader(st->scan, &st->spec);
			MemoryContextSwitchTo(old);
			if (st->reader == NULL)
			{
				st->done = true;
				return false;
			}
		}
		if (!lt_reader_next_batch(st->reader))
		{
			lt_reader_end(st->reader);
			st->reader = NULL;
			continue;
		}
		/* the reader's batch moves to a holder, which owns it from here */
		st->current = MemoryContextAllocZero(st->cxt, sizeof(Holder));
		st->current->batch = st->reader->batch;
		st->current->refs = 1;
		memset(&st->reader->batch, 0, sizeof(LtBatch));
		st->pos = 0;
	}

	if (out == NULL)
	{
		out = MemoryContextAllocZero(st->cxt, sizeof(SrcOut));
		out->cxt = AllocSetContextCreate(st->cxt, "lance_tam vexec batch", ALLOCSET_DEFAULT_SIZES);
		st->outs = lappend(st->outs, out);
	}
	out->holder = st->current;
	st->current->refs++;
	batch = &st->current->batch;
	nrows = (int) Min((int64) st->max_rows, batch->nrows - st->pos);
	out->columns = MemoryContextAllocZero(out->cxt, sizeof(VexecColumn) * Max(st->ncols, 1));
	for (int i = 0; i < st->ncols; i++)
	{
		int			ci = -1;

		for (int j = 0; j < batch->ncols; j++)
			if (batch->cols[j] && batch->cols[j]->attnum == st->attnums[i])
				ci = j;
		if (ci < 0)
		{
			/* a column the dataset lacks: every row NULL */
			static const uint64 nulls[1] = {0};
			Datum	   *datums = MemoryContextAllocZero(out->cxt, sizeof(Datum));

			out->columns[i].layout = VEXEC_DATUM;
			out->columns[i].encoding = VEXEC_CONST;
			out->columns[i].validity = nulls;
			out->columns[i].values = datums;
			continue;
		}
		fill_column(out, batch, ci, st->pos, nrows, &out->columns[i]);
	}
	if (st->want_tids)
	{
		out->tids = MemoryContextAlloc(out->cxt, sizeof(ItemPointerData) * Max(nrows, 1));
		for (int i = 0; i < nrows; i++)
			lt_rowid_to_tid(lt_batch_rowid(batch, st->pos + i), &out->tids[i]);
	}
	result->nrows = nrows;
	result->visible = NULL;
	result->tids = out->tids;
	result->columns = out->columns;
	result->owner = out;
	st->pos += nrows;
	st->last = out;
	if (pgstat_should_count_relation(st->rel))
		st->rel->pgstat_info->counts.tuples_returned += nrows;
	return true;
}

static void
src_retain(void *arg, void *owner)
{
	((SrcOut *) owner)->retained = true;
}

static void
src_release(void *arg, void *owner)
{
	SrcState   *st = (SrcState *) arg;
	SrcOut	   *out = (SrcOut *) owner;

	if (!out->retained)
		return;
	out->retained = false;
	if (out != st->last)
		out_reset(out);
}

static void
src_rescan(void *arg)
{
	SrcState   *st = (SrcState *) arg;

	if (st->reader)
		lt_reader_end(st->reader);
	st->reader = NULL;
	if (st->current)
		holder_unref(st->current);
	st->current = NULL;
	st->done = false;
	st->pos = 0;
	lt_scan_reset_take(st->scan);
}

static void
src_end(void *arg)
{
	SrcState   *st = (SrcState *) arg;
	ListCell   *lc;

	if (st->reader)
		lt_reader_end(st->reader);
	st->reader = NULL;
	foreach(lc, st->outs)
		out_reset((SrcOut *) lfirst(lc));
	if (st->current)
		holder_unref(st->current);
	st->current = NULL;
	MemoryContextDelete(st->cxt);
}

/*
 * The cost model's estimate: the version's rows, and the bytes of the
 * needed columns, their share of the data's by the columns' widths.
 */
static void
src_estimate(Relation rel, Snapshot snapshot, const VexecSourceSpec *spec, double *rows,
			 double *bytes)
{
	LtTable    *t = lt_table(rel);
	uint64		version = lt_snapshot_version(rel, t, snapshot);
	LeDataset  *ds = lt_open_dataset(t, version);
	LeDatasetInfo info;
	LeError		err = {0};
	TupleDesc	desc = RelationGetDescr(rel);
	double		total = 0;
	double		needed = 0;
	int			rc;

	memset(&info, 0, sizeof(info));
	rc = lt_api()->dataset_info(ds, &info, &err);
	lt_api()->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "estimating a scan of a lance table");
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		double		w;

		if (att->attisdropped)
			continue;
		w = att->attlen > 0 ? att->attlen : get_typavgwidth(att->atttypid, att->atttypmod);
		total += w;
		for (int j = 0; j < spec->ncolumns; j++)
			if (spec->attnums[j] == att->attnum)
				needed += w;
	}
	*rows = (double) info.num_rows;
	*bytes = total > 0 ? (double) info.data_bytes * Max(needed, 1) / total : (double) info.data_bytes;
}

static const VexecSourceRoutine src_routine = {
	.size = sizeof(VexecSourceRoutine),
	.minor = VEXEC_SOURCE_MINOR,
	.am = NULL,					/* set at registration */
	.name = "lance",
	.supports = src_supports,
	.begin = src_begin,
	.next = src_next,
	.retain = src_retain,
	.release = src_release,
	.rescan = src_rescan,
	.end = src_end,
	.estimate = src_estimate,
	.aggregate = NULL,
	.set_keys = NULL,
};

/* ------------------------------------------------------------------------
 * The sink
 * ------------------------------------------------------------------------
 */

typedef struct SinkState
{
	Relation	rel;
	int			natts;
	VexecSinkLayout *layouts;
	CommandId	cid;
	bool		tids;
	TupleTableSlot *slot;
	MemoryContext rowcxt;
	LtLayout   *layout;
} SinkState;

static bool
sink_supports(Relation rel, AttrNumber attnum, VexecSinkLayout *layout)
{
	Form_pg_attribute att = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
	int32		typmod = att->atttypmod;
	Oid			typid = getBaseTypeAndTypmod(att->atttypid, &typmod);

	memset(layout, 0, sizeof(*layout));
	switch (typid)
	{
		case BOOLOID:
			layout->layout = VEXEC_BYTE_BOOL;
			return true;
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case BYTEAOID:
			layout->layout = VEXEC_OFFSETS;
			return true;
		case NUMERICOID:
			if (typmod >= (int32) VARHDRSZ)
			{
				int32		precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;
				int32		scale = (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;

				if (precision >= 1 && precision <= 38 && scale >= 0 && scale <= precision)
				{
					layout->layout = VEXEC_SCALED;
					layout->width = precision <= 18 ? 8 : 16;
					layout->stride = layout->width;
					layout->scale = (int16) scale;
					return true;
				}
			}
			layout->layout = VEXEC_DATUM;
			return true;
		default:
			/* by value, or of a fixed length (uuid, interval): its bytes */
			if (att->attlen > 0)
			{
				layout->layout = VEXEC_FIXED;
				layout->width = att->attlen;
				layout->stride = att->attlen;
				return true;
			}
			layout->layout = VEXEC_DATUM;
			return true;
	}
}

static void *
sink_begin(Relation rel, const VexecSinkSpec *spec)
{
	SinkState  *st = palloc0(sizeof(SinkState));

	st->rel = rel;
	st->natts = spec->natts;
	st->layouts = palloc(sizeof(VexecSinkLayout) * Max(spec->natts, 1));
	memcpy(st->layouts, spec->layouts, sizeof(VexecSinkLayout) * spec->natts);
	st->cid = spec->cid;
	st->tids = (spec->flags & VEXEC_SINK_TIDS) != 0;
	st->slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsVirtual);
	st->rowcxt = AllocSetContextCreate(CurrentMemoryContext, "lance_tam vexec sink row",
									   ALLOCSET_DEFAULT_SIZES);
	return st;
}

/* A value of a column at row i, as a Datum of the attribute's type */
static Datum
sink_value(const VexecColumn *c, Form_pg_attribute att, int i, bool *isnull)
{
	int			row = c->encoding == VEXEC_CONST ? 0 : i;

	if (c->validity && !((c->validity[row >> 6] >> (row & 63)) & 1))
	{
		*isnull = true;
		return (Datum) 0;
	}
	*isnull = false;
	switch (c->layout)
	{
		case VEXEC_FIXED:
			{
				const char *p = ((const char *) c->values) + (size_t) row * c->stride;

				if (att->attbyval)
					return fetch_att(p, true, att->attlen);
				return PointerGetDatum(p);
			}
		case VEXEC_BYTE_BOOL:
			return BoolGetDatum(((const uint8 *) c->values)[row] != 0);
		case VEXEC_BIT_BOOL:
			return BoolGetDatum((((const uint64 *) c->values)[row >> 6] >> (row & 63)) & 1);
		case VEXEC_OFFSETS:
			{
				const int32 *off = (const int32 *) c->values;
				const char *data = (const char *) c->buffers[0];
				int32		len = off[row + 1] - off[row];
				struct varlena *v = palloc(VARHDRSZ + len);

				SET_VARSIZE(v, VARHDRSZ + len);
				memcpy(VARDATA(v), data + off[row], len);
				return PointerGetDatum(v);
			}
		case VEXEC_SCALED:
			{
				uint8		v[16];
				int32		typmod = att->atttypmod;
				int32		precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;

				if (c->width == 8)
				{
					__int128	w = ((const int64 *) c->values)[row];

					memcpy(v, &w, 16);
				}
				else
					memcpy(v, ((const uint8 *) c->values) + 16 * (size_t) row, 16);
				return lt_int128_to_numeric(v, precision, c->scale);
			}
		case VEXEC_DATUM:
			return ((const Datum *) c->values)[row];
		default:
			elog(ERROR, "lance_tam: a vexec sink column of layout %d", c->layout);
	}
	pg_unreachable();
}

static void
sink_put(void *arg, VexecSinkBatch *batch)
{
	SinkState  *st = (SinkState *) arg;
	TupleDesc	desc = RelationGetDescr(st->rel);

	for (int i = 0; i < batch->nrows; i++)
	{
		MemoryContext old;

		CHECK_FOR_INTERRUPTS();
		MemoryContextReset(st->rowcxt);
		old = MemoryContextSwitchTo(st->rowcxt);
		ExecClearTuple(st->slot);
		for (int a = 0; a < desc->natts; a++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, a);

			if (att->attisdropped)
			{
				st->slot->tts_isnull[a] = true;
				st->slot->tts_values[a] = (Datum) 0;
				continue;
			}
			st->slot->tts_values[a] = sink_value(&batch->columns[a], att, i,
												 &st->slot->tts_isnull[a]);
		}
		ExecStoreVirtualTuple(st->slot);
		MemoryContextSwitchTo(old);
		lt_xact_insert(st->rel, st->slot, st->cid);
		if (st->tids && batch->tids)
			batch->tids[i] = st->slot->tts_tid;
	}
	pgstat_count_heap_insert(st->rel, batch->nrows);
}

static void
sink_end(void *arg)
{
	SinkState  *st = (SinkState *) arg;

	ExecDropSingleTupleTableSlot(st->slot);
	MemoryContextDelete(st->rowcxt);
	pfree(st->layouts);
	pfree(st);
}

static const VexecSinkRoutine sink_routine = {
	.size = sizeof(VexecSinkRoutine),
	.minor = VEXEC_SINK_MINOR,
	.am = NULL,					/* set at registration */
	.name = "lance",
	.supports = sink_supports,
	.begin = sink_begin,
	.put = sink_put,
	.end = sink_end,
};

static VexecSourceRoutine registered_source;
static VexecSinkRoutine registered_sink;

void
lt_vexec_init(void)
{
	registered_source = src_routine;
	registered_source.am = lt_tableam();
	vexec_register_source(&registered_source);
	registered_sink = sink_routine;
	registered_sink.am = lt_tableam();
	vexec_register_sink(&registered_sink);
}
