/*-------------------------------------------------------------------------
 *
 * scan.c
 *	  Reading a lance table (docs/design.md §2.8): the reader the access
 *	  method's scans, LanceScan and vexec's source share, and fetches by
 *	  TID.
 *
 *	  A reader reads the version its snapshot's catalog row names, through
 *	  the engine's scan of it, with the transaction's own changes added:
 *	  its rows of earlier commands, less the rows it deleted in them, as a
 *	  heap scan with the snapshot's command id sees its own.  The batches
 *	  come in the types the reader asked for, and each row becomes a
 *	  virtual slot whose values live until the next row.
 *
 *	  A fetch by TID is served from the batch an active reader of the
 *	  relation holds when the row is in it -- ModifyTable fetches the row an
 *	  UPDATE changes, or a DELETE with RETURNING or row triggers removes,
 *	  right after its scan read it -- and from the engine's take otherwise.
 *	  A row id names one version of a row, never changed, so the batch's
 *	  values are the row's whichever snapshot asks; the snapshot decides
 *	  only whether the row is visible, and the batch's reader read it with
 *	  its own.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc_details.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

#include "lance_tam.h"

/* Readers holding a batch a fetch can be served from */
#define MAX_ACTIVE_READERS 16
static LtReader *active_readers[MAX_ACTIVE_READERS];
static int	nactive = 0;

static void
register_reader(LtReader *r)
{
	if (nactive < MAX_ACTIVE_READERS)
		active_readers[nactive++] = r;
}

static void
unregister_reader(LtReader *r)
{
	for (int i = 0; i < nactive; i++)
	{
		if (active_readers[i] == r)
		{
			active_readers[i] = active_readers[--nactive];
			return;
		}
	}
}

/* A transaction's end leaves no reader behind */
void
lt_scan_reset_readers(void)
{
	nactive = 0;
}

/*
 * A reader's engine handles, malloc'd and kept by the resource owner the
 * reader began under, so that an error ending the scan releases them: the
 * engine's scan task, and the dataset it reads.
 */
typedef struct EngineHandles
{
	LeScan	   *scan;
	LeDataset  *dataset;
	LtReader   *reader;
	ResourceOwner owner;
} EngineHandles;

static void
release_handles(EngineHandles *h)
{
	if (h->reader)
		unregister_reader(h->reader);
	if (lt_engine_loaded())
	{
		if (h->scan)
			lt_api()->scan_end(h->scan);
		if (h->dataset)
			lt_api()->dataset_close(h->dataset);
	}
	free(h);
}

static void
resowner_release(Datum res)
{
	release_handles((EngineHandles *) DatumGetPointer(res));
}

static const ResourceOwnerDesc handles_desc = {
	.name = "lance_tam scan",
	.release_phase = RESOURCE_RELEASE_BEFORE_LOCKS,
	.release_priority = RELEASE_PRIO_FIRST,
	.ReleaseResource = resowner_release,
	.DebugPrint = NULL
};

LeDataset *
lt_open_dataset(LtTable *t, uint64 version)
{
	LeDataset  *ds = NULL;
	LeError		err = {0};

	if (lt_api()->dataset_open(t->uri, t->storage_json, (int64) version, &ds, &err) != LE_OK)
		lt_raise(&err, "opening a lance table's dataset");
	return ds;
}

/*
 * The command id a snapshot sees the transaction's own changes before: an
 * MVCC snapshot's own; SnapshotSelf's and SnapshotDirty's, every change so
 * far.  SnapshotAny sees every row version, the transaction's own deleted
 * ones too: -1, which the engine reads as every row and no deletion.
 */
static int64
snapshot_cid(Snapshot snapshot)
{
	if (snapshot && IsMVCCSnapshot(snapshot))
		return (int64) snapshot->curcid;
	if (snapshot == SnapshotAny)
		return -1;
	return PG_INT64_MAX;
}

static void
fill_columns(LtReader *r, List *attnums)
{
	ListCell   *lc;
	int			n = 0;

	r->attidx = palloc(sizeof(int) * Max(r->layout->natts, 1));
	r->cols = palloc(sizeof(LtColumn *) * Max(r->layout->natts, 1));
	if (attnums == NIL)
	{
		r->all_attrs = true;
		for (int i = 0; i < r->layout->natts; i++)
		{
			if (r->layout->cols[i].kind == LT_NONE)
				continue;
			r->attidx[n] = i;
			r->cols[n] = &r->layout->cols[i];
			n++;
		}
	}
	else
	{
		int			nattrs = 0;

		foreach(lc, attnums)
		{
			int			ai = lfirst_int(lc) - 1;

			if (ai < 0 || ai >= r->layout->natts)
				continue;
			nattrs++;
			if (r->layout->cols[ai].kind == LT_NONE)
				continue;
			r->attidx[n] = ai;
			r->cols[n] = &r->layout->cols[ai];
			n++;
		}
		r->all_attrs = nattrs >= r->layout->natts;
	}
	r->ncols = n;

	/*
	 * The attributes the dataset has no column for whose rows PostgreSQL
	 * gives a value: the default ALTER TABLE ... ADD COLUMN keeps as
	 * attmissingval, read so -- as heap reads it -- until the column is
	 * the dataset's, at the statement's end.
	 */
	{
		TupleDesc	desc = RelationGetDescr(r->rel);

		r->nmissing = 0;
		if (desc->constr != NULL && desc->constr->missing != NULL)
		{
			r->missing_attidx = palloc(sizeof(int) * Max(r->layout->natts, 1));
			for (int i = 0; i < r->layout->natts && i < desc->natts; i++)
			{
				if (r->layout->cols[i].kind == LT_NONE &&
					!TupleDescAttr(desc, i)->attisdropped &&
					desc->constr->missing[i].am_present)
					r->missing_attidx[r->nmissing++] = i;
			}
		}
	}
}

void
lt_reader_fill_spec(LtReader *r, LtReadSpec *rs, LeScanSpec *spec, struct ArrowSchema *types)
{
	const char **names = palloc(sizeof(char *) * Max(r->ncols, 1));
	LtXactTable *xt;

	for (int i = 0; i < r->ncols; i++)
		names[i] = r->cols[i]->name;
	memset(spec, 0, sizeof(*spec));
	spec->size = sizeof(*spec);
	spec->ncolumns = r->ncols;
	spec->columns = names;
	lt_export_schema(r->layout, r->ncols, r->attidx, types);
	spec->types = types;
	spec->filter = rs ? rs->filter : NULL;
	spec->limit = rs ? rs->limit : -1;
	spec->offset = 0;
	spec->with_row_id = true;
	spec->scan_in_order = lt_scan_in_order;
	spec->batch_size = lt_batch_rows;
	spec->readahead = lt_readahead;
	spec->nfragments = -1;
	if (rs && rs->fragments != NIL)
	{
		uint64	   *frags = palloc(sizeof(uint64) * list_length(rs->fragments));
		ListCell   *lc;
		int			i = 0;

		foreach(lc, rs->fragments)
			frags[i++] = (uint64) lfirst_int(lc);
		spec->nfragments = i;
		spec->fragments = frags;
	}
	if (rs && rs->nn_attnum > 0)
	{
		LtColumn   *col = &r->layout->cols[rs->nn_attnum - 1];

		spec->nn_column = col->name;
		if (rs->nn_query_half)
		{
			spec->nn_query = rs->nn_query_half;
			spec->nn_query_type = LE_VEC_FLOAT16;
		}
		else
		{
			spec->nn_query = rs->nn_query;
			spec->nn_query_type = LE_VEC_FLOAT32;
		}
		spec->nn_dim = rs->nn_dim;
		spec->nn_k = rs->nn_k;
		spec->nn_metric = rs->nn_metric;
		spec->nn_nprobes = lt_nprobes;
		spec->nn_ef = lt_ef;
		spec->nn_refine = lt_refine_factor;
		spec->nn_use_index = true;
		spec->prefilter = rs->prefilter;
	}
	if (rs && rs->fts_query)
	{
		spec->fts_query = rs->fts_query;
		spec->fts_column = rs->fts_attnum > 0 ? r->layout->cols[rs->fts_attnum - 1].name : NULL;
		spec->prefilter = rs->prefilter;
	}
	xt = lt_xact_table_by_relid(RelationGetRelid(r->rel));
	if (xt && xt->write)
	{
		lt_xact_flush(xt);
		spec->write = xt->write;
	}
	spec->cid = snapshot_cid(r->snapshot);
}

LtReader *
lt_reader_begin(Relation rel, Snapshot snapshot, LtReadSpec *rs)
{
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext, "lance_tam reader",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContext old = MemoryContextSwitchTo(cxt);
	LtReader   *r = palloc0(sizeof(LtReader));
	LeScanSpec	spec;
	struct ArrowSchema types;
	LeError		err = {0};
	int			rc;
	EngineHandles *h;

	r->rel = rel;
	r->snapshot = snapshot;
	r->table = lt_table(rel);
	r->layout = lt_layout_build(rel, cxt);
	r->rowcxt = AllocSetContextCreate(cxt, "lance_tam row", ALLOCSET_DEFAULT_SIZES);
	fill_columns(r, rs ? rs->attnums : NIL);
	r->version = (rs && rs->version) ? rs->version : lt_snapshot_version(rel, r->table, snapshot);

	ResourceOwnerEnlarge(CurrentResourceOwner);
	h = calloc(1, sizeof(EngineHandles));
	if (h == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	h->owner = CurrentResourceOwner;
	h->reader = r;
	ResourceOwnerRemember(h->owner, PointerGetDatum(h), &handles_desc);
	r->handles = h;

	h->dataset = r->dataset = lt_open_dataset(r->table, r->version);
	memset(&types, 0, sizeof(types));
	lt_reader_fill_spec(r, rs, &spec, &types);
	rc = lt_api()->scan_begin(r->dataset, &spec, &r->scan, &err);
	if (types.release)
		types.release(&types);
	if (rc != LE_OK)
		lt_raise(&err, "beginning a scan of a lance table");
	h->scan = r->scan;
	MemoryContextSwitchTo(old);
	register_reader(r);
	return r;
}

bool
lt_reader_next_batch(LtReader *r)
{
	LeError		err = {0};
	bool		done = false;

	lt_batch_release(&r->batch);
	r->row = 0;
	if (r->done)
		return false;
	memset(&r->batch.array, 0, sizeof(r->batch.array));
	memset(&r->batch.schema, 0, sizeof(r->batch.schema));
	if (lt_api()->scan_next(r->scan, &r->batch.array, &r->batch.schema, &done, &err) != LE_OK)
		lt_raise(&err, "reading a lance table");
	if (done)
	{
		r->done = true;
		return false;
	}
	r->batch.valid = true;
	lt_batch_bind(&r->batch, r->ncols, r->cols);
	r->batches_read++;
	return true;
}

/* A row of a batch into a slot: the attributes read, the others NULL */
void
lt_store_batch_row(LtBatch *batch, int64 row, LtColumn **cols, const int *attidx, int ncols,
		  TupleTableSlot *slot)
{
	TupleDesc	desc = slot->tts_tupleDescriptor;

	ExecClearTuple(slot);
	memset(slot->tts_isnull, true, sizeof(bool) * desc->natts);
	for (int i = 0; i < ncols; i++)
	{
		int			ai = attidx[i];

		if (ai < desc->natts)
			slot->tts_values[ai] = lt_batch_datum(batch, i, row, &slot->tts_isnull[ai]);
	}
	if (batch->rowid_index >= 0)
		lt_rowid_to_tid(lt_batch_rowid(batch, row), &slot->tts_tid);
	else
		ItemPointerSetInvalid(&slot->tts_tid);
	ExecStoreVirtualTuple(slot);
}

/* A row of a reader's batch into a slot */
void
lt_reader_store_row(LtReader *r, LtBatch *batch, int64 row, TupleTableSlot *slot)
{
	lt_store_batch_row(batch, row, r->cols, r->attidx, r->ncols, slot);
	for (int i = 0; i < r->nmissing; i++)
	{
		int			ai = r->missing_attidx[i];

		if (ai < slot->tts_tupleDescriptor->natts)
			slot->tts_values[ai] = getmissingattr(RelationGetDescr(r->rel), ai + 1,
												  &slot->tts_isnull[ai]);
	}
}

bool
lt_reader_next(LtReader *r, TupleTableSlot *slot)
{
	MemoryContext old;

	CHECK_FOR_INTERRUPTS();
	MemoryContextReset(r->rowcxt);
	while (!r->batch.valid || r->row >= r->batch.nrows)
	{
		if (!lt_reader_next_batch(r))
		{
			ExecClearTuple(slot);
			return false;
		}
	}
	old = MemoryContextSwitchTo(r->rowcxt);
	lt_reader_store_row(r, &r->batch, r->row, slot);
	MemoryContextSwitchTo(old);
	slot->tts_tableOid = RelationGetRelid(r->rel);
	r->row++;
	r->rows_read++;
	return true;
}

void
lt_reader_end(LtReader *r)
{
	EngineHandles *h;

	if (r == NULL)
		return;
	lt_batch_release(&r->batch);
	h = (EngineHandles *) r->handles;
	if (h)
	{
		ResourceOwnerForget(h->owner, PointerGetDatum(h), &handles_desc);
		release_handles(h);
	}
	r->handles = NULL;
	r->scan = NULL;
	r->dataset = NULL;
	MemoryContextDelete(GetMemoryChunkContext(r));
}

char *
lt_reader_explain(Relation rel, Snapshot snapshot, LtReadSpec *rs, bool verbose)
{
	LtReader	r;
	LeScanSpec	spec;
	struct ArrowSchema types;
	LeError		err = {0};
	char	   *out = NULL;
	int			rc;

	memset(&r, 0, sizeof(r));
	r.rel = rel;
	r.snapshot = snapshot;
	r.table = lt_table(rel);
	r.layout = lt_layout(rel);
	fill_columns(&r, rs ? rs->attnums : NIL);
	r.version = lt_snapshot_version(rel, r.table, snapshot);
	r.dataset = lt_open_dataset(r.table, r.version);
	memset(&types, 0, sizeof(types));
	lt_reader_fill_spec(&r, rs, &spec, &types);
	rc = lt_api()->scan_explain(r.dataset, &spec, verbose, &out, &err);
	if (types.release)
		types.release(&types);
	lt_api()->dataset_close(r.dataset);
	if (rc != LE_OK)
		lt_raise(&err, "explaining a scan of a lance table");
	return lt_take_string(out);
}

/* ------------------------------------------------------------------------
 * Fetches by TID
 * ------------------------------------------------------------------------
 */

static bool
fetch_from_readers(Relation rel, Snapshot snapshot, uint64 rowid, TupleTableSlot *slot)
{
	for (int i = 0; i < nactive; i++)
	{
		LtReader   *r = active_readers[i];
		int64		row = -1;

		if (r->rel->rd_id != rel->rd_id || !r->all_attrs || !r->batch.valid ||
			r->batch.rowid_index < 0)
			continue;
		if (snapshot != SnapshotAny && snapshot != r->snapshot)
			continue;
		/* most often, the row the reader handed last */
		if (r->row > 0 && lt_batch_rowid(&r->batch, r->row - 1) == rowid)
			row = r->row - 1;
		else
		{
			for (int64 j = 0; j < r->batch.nrows; j++)
			{
				if (lt_batch_rowid(&r->batch, j) == rowid)
				{
					row = j;
					break;
				}
			}
		}
		if (row < 0)
			continue;
		lt_reader_store_row(r, &r->batch, row, slot);
		slot->tts_tableOid = RelationGetRelid(rel);
		/* the values must outlive the reader's next row */
		ExecMaterializeSlot(slot);
		return true;
	}
	return false;
}

/*
 * Rows by row id, as a snapshot sees them, into slots; found[i] says
 * whether rowids[i] named a visible row.  Returns whether any did.
 */
bool
lt_fetch_rows(Relation rel, Snapshot snapshot, const uint64 *rowids, int n,
			  TupleTableSlot **slots, bool *found)
{
	MemoryContext cxt;
	MemoryContext old;
	LtReader	r;
	LeScanSpec	spec;
	struct ArrowSchema types;
	LtBatch		batch;
	LeError		err = {0};
	bool		any = false;
	int			rc;
	int			missing = 0;

	for (int i = 0; i < n; i++)
	{
		found[i] = fetch_from_readers(rel, snapshot, rowids[i], slots[i]);
		if (found[i])
			any = true;
		else
			missing++;
	}
	if (missing == 0)
		return any;

	cxt = AllocSetContextCreate(CurrentMemoryContext, "lance_tam fetch", ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(cxt);
	memset(&r, 0, sizeof(r));
	r.rel = rel;
	r.snapshot = snapshot;
	r.table = lt_table(rel);
	r.layout = lt_layout_build(rel, cxt);
	fill_columns(&r, NIL);
	r.version = lt_snapshot_version(rel, r.table, snapshot);
	r.dataset = lt_open_dataset(r.table, r.version);
	memset(&types, 0, sizeof(types));
	lt_reader_fill_spec(&r, NULL, &spec, &types);
	{
		uint64	   *want = palloc(sizeof(uint64) * missing);
		int			k = 0;

		for (int i = 0; i < n; i++)
			if (!found[i])
				want[k++] = rowids[i];
		memset(&batch, 0, sizeof(batch));
		rc = lt_api()->take(r.dataset, &spec, want, k, &batch.array, &batch.schema, &err);
	}
	if (types.release)
		types.release(&types);
	lt_api()->dataset_close(r.dataset);
	if (rc != LE_OK)
	{
		MemoryContextSwitchTo(old);
		lt_raise(&err, "fetching rows of a lance table");
	}
	batch.valid = true;
	lt_batch_bind(&batch, r.ncols, r.cols);
	MemoryContextSwitchTo(old);
	PG_TRY();
	{
		for (int64 row = 0; row < batch.nrows; row++)
		{
			uint64		id = lt_batch_rowid(&batch, row);

			for (int i = 0; i < n; i++)
			{
				if (found[i] || rowids[i] != id)
					continue;
				lt_reader_store_row(&r, &batch, row, slots[i]);
				slots[i]->tts_tableOid = RelationGetRelid(rel);
				ExecMaterializeSlot(slots[i]);
				found[i] = true;
				any = true;
			}
		}
	}
	PG_FINALLY();
	{
		lt_batch_release(&batch);
		MemoryContextDelete(cxt);
	}
	PG_END_TRY();
	return any;
}
