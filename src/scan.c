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
 *	  right after its scan read it -- from a window of rows taken ahead of
 *	  fetches that come in the order of their TIDs -- AFTER triggers', a
 *	  foreign key's check of each row a statement wrote -- and from the
 *	  engine's take otherwise.
 *	  A row updated in place keeps its id, so a row id names the row's
 *	  versions, the take giving the one the snapshot sees; a batch serves
 *	  the fetches of its reader's snapshot, which sees the version it read,
 *	  and SnapshotAny's, which ModifyTable's of the row its scan just read
 *	  are.
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

static void reset_windows(void);

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

/* A transaction's end leaves no reader behind, nor a fetch window */
void
lt_scan_reset_readers(void)
{
	nactive = 0;
	reset_windows();
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

/*
 * The row of an active reader's batch a fetch by the snapshot can be served
 * from, and its reader; -1 when no reader has it.
 */
static int64
reader_row(Relation rel, Snapshot snapshot, uint64 rowid, LtReader **reader)
{
	for (int i = 0; i < nactive; i++)
	{
		LtReader   *r = active_readers[i];

		if (r->rel->rd_id != rel->rd_id || !r->all_attrs || !r->batch.valid ||
			r->batch.rowid_index < 0)
			continue;
		if (snapshot != SnapshotAny && snapshot != r->snapshot)
			continue;
		*reader = r;
		/* most often, the row the reader handed last */
		if (r->row > 0 && lt_batch_rowid(&r->batch, r->row - 1) == rowid)
			return r->row - 1;
		for (int64 j = 0; j < r->batch.nrows; j++)
			if (lt_batch_rowid(&r->batch, j) == rowid)
				return j;
	}
	return -1;
}

static bool
fetch_from_readers(Relation rel, Snapshot snapshot, uint64 rowid, TupleTableSlot *slot)
{
	LtReader   *r = NULL;
	int64		row = reader_row(rel, snapshot, rowid, &r);

	if (row < 0)
		return false;
	lt_reader_store_row(r, &r->batch, row, slot);
	slot->tts_tableOid = RelationGetRelid(rel);
	/* the values must outlive the reader's next row */
	ExecMaterializeSlot(slot);
	return true;
}

/* A value's bytes in a column of the kind, a variable width's counted wide */
static int
kind_width(LtKind kind)
{
	switch (kind)
	{
		case LT_BOOL:
			return 1;
		case LT_INT2:
			return 2;
		case LT_INT4:
		case LT_FLOAT4:
		case LT_DATE:
			return 4;
		case LT_INT8:
		case LT_FLOAT8:
		case LT_TIME:
		case LT_TIMESTAMP:
		case LT_TIMESTAMPTZ:
			return 8;
		case LT_NUMERIC:
		case LT_UUID:
			return 16;
		default:
			return 1024;
	}
}

/*
 * What a take that asks only which rows a snapshot sees reads -- a take
 * reads a column at least -- the dataset's narrowest column; NIL, every
 * column, when it has none.
 */
static List *
narrowest_column(LtLayout *layout)
{
	int			best = 0;
	int			best_width = INT_MAX;

	for (int i = 0; i < layout->natts; i++)
	{
		int			width;

		if (layout->cols[i].kind == LT_NONE)
			continue;
		width = kind_width(layout->cols[i].kind);
		if (width < best_width)
		{
			best = i + 1;
			best_width = width;
		}
	}
	return best > 0 ? list_make1_int(best) : NIL;
}

/*
 * The rows "want" names, as the snapshot sees them at "version", by the
 * engine's take, into "batch" for reader "r", made in the current memory
 * context: every column, or with "narrow" the narrowest.
 */
static void
take_rows(Relation rel, Snapshot snapshot, uint64 version, bool narrow, const uint64 *want,
		  int n, LtReader *r, LtBatch *batch)
{
	LeScanSpec	spec;
	struct ArrowSchema types;
	LeError		err = {0};
	int			rc;

	memset(r, 0, sizeof(*r));
	r->rel = rel;
	r->snapshot = snapshot;
	r->table = lt_table(rel);
	r->layout = lt_layout_build(rel, CurrentMemoryContext);
	fill_columns(r, narrow ? narrowest_column(r->layout) : NIL);
	r->version = version;
	r->dataset = lt_open_dataset(r->table, r->version);
	memset(&types, 0, sizeof(types));
	lt_reader_fill_spec(r, NULL, &spec, &types);
	memset(batch, 0, sizeof(*batch));
	rc = lt_api()->take(r->dataset, &spec, want, n, &batch->array, &batch->schema, &err);
	if (types.release)
		types.release(&types);
	lt_api()->dataset_close(r->dataset);
	r->dataset = NULL;
	if (rc != LE_OK)
		lt_raise(&err, "fetching rows of a lance table");
	batch->valid = true;
	lt_batch_bind(batch, r->ncols, r->cols);
}

/*
 * A window: the rows of a run of row ids, taken ahead of fetches that ask
 * for a table's rows in the order of their TIDs -- the AFTER triggers' of
 * the rows a statement wrote, among them a foreign key's check of each --
 * so that such a fetch searches a batch instead of making a take of its
 * own, which costs about 0.1 ms however few rows it takes.  The second
 * fetch of a run of consecutive row ids takes a window from its row on:
 * 16 rows, four times as many each time the run uses one up, at most
 * lance.fetch_ahead and lance.batch_rows.  A window answers for its table,
 * the snapshot's command (snapshot_cid()) and version, and the
 * transaction's changes to the table when it was taken
 * (lt_xact_generation()): an id it took without a row names a row the
 * snapshot does not see.  A change of the table between two fetches -- an
 * UPDATE's of the rows it fetches -- ends the run, so that those fetches
 * take a row at a time.
 */
typedef struct FetchWindow
{
	MemoryContext cxt;			/* the reader's columns; NULL: none */
	MemoryContext rowcxt;		/* a fetched row's values, until the next */
	LtReader	r;
	LtBatch		batch;
	bool		taken;
	Oid			relid;
	int64		cid;
	uint64		version;
	uint64		generation;
	uint64		first;			/* the ids taken, first to last */
	uint64		last;
	/* the run: its last fetch's table, command, row id and changes */
	Oid			run_relid;
	int64		run_cid;
	uint64		run_rowid;
	uint64		run_generation;
} FetchWindow;

#define FETCH_WINDOW_FIRST	16

/* SnapshotAny's fetches, other snapshots', and checks of visibility */
#define WINDOW_ANY			0
#define WINDOW_SNAPSHOT		1
#define WINDOW_VISIBLE		2
#define NWINDOWS			3

static FetchWindow windows[NWINDOWS];

static void
window_release(FetchWindow *w)
{
	if (w->cxt == NULL)
		return;
	lt_batch_release(&w->batch);
	MemoryContextDelete(w->cxt);
	w->cxt = NULL;
	w->rowcxt = NULL;
	w->taken = false;
}

/* A transaction's end: the windows go with it */
static void
reset_windows(void)
{
	for (int i = 0; i < NWINDOWS; i++)
	{
		window_release(&windows[i]);
		memset(&windows[i], 0, sizeof(windows[i]));
	}
}

static bool
window_covers(FetchWindow *w, Oid relid, int64 cid, uint64 version, uint64 generation,
			  uint64 rowid)
{
	return w->taken && w->relid == relid && w->cid == cid && w->version == version &&
		w->generation == generation && rowid >= w->first && rowid <= w->last;
}

/* The window's row of the id, -1 when the snapshot sees none */
static int64
window_row(FetchWindow *w, uint64 rowid)
{
	int64		lo = 0;
	int64		hi = w->batch.nrows - 1;

	/* the take gives the rows in the order of the ids asked for */
	while (lo <= hi)
	{
		int64		mid = lo + (hi - lo) / 2;
		uint64		id = lt_batch_rowid(&w->batch, mid);

		if (id == rowid)
			return mid;
		if (id < rowid)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return -1;
}

static void
window_take(FetchWindow *w, Relation rel, Snapshot snapshot, uint64 version, uint64 generation,
			uint64 first, int rows, bool narrow)
{
	MemoryContext old;
	uint64	   *want;

	window_release(w);
	w->cxt = AllocSetContextCreate(TopTransactionContext, "lance_tam fetch window",
								   ALLOCSET_DEFAULT_SIZES);
	w->rowcxt = AllocSetContextCreate(w->cxt, "lance_tam fetch window row",
									  ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(w->cxt);
	want = palloc(sizeof(uint64) * rows);
	for (int i = 0; i < rows; i++)
		want[i] = first + i;
	take_rows(rel, snapshot, version, narrow, want, rows, &w->r, &w->batch);
	pfree(want);
	MemoryContextSwitchTo(old);
	w->taken = true;
	w->relid = RelationGetRelid(rel);
	w->cid = snapshot_cid(snapshot);
	w->version = version;
	w->generation = generation;
	w->first = first;
	w->last = first + rows - 1;
}

/*
 * The window of the kind "w" that answers a fetch of the row id, taken now
 * when the fetch continues a run; NULL when none does.
 */
static FetchWindow *
window_for(FetchWindow *w, Relation rel, Snapshot snapshot, uint64 version, uint64 rowid,
		   bool narrow)
{
	Oid			relid = RelationGetRelid(rel);
	int64		cid = snapshot_cid(snapshot);
	uint64		generation = lt_xact_generation(relid);
	bool		in_run = w->run_relid == relid && w->run_cid == cid &&
		w->run_rowid + 1 == rowid && w->run_generation == generation;
	int			most = Min(lt_fetch_ahead, lt_batch_rows);
	int			rows;

	w->run_relid = relid;
	w->run_cid = cid;
	w->run_rowid = rowid;
	w->run_generation = generation;
	if (window_covers(w, relid, cid, version, generation, rowid))
		return w;
	if (!in_run || most < 2)
		return NULL;
	/* the run used up the window it had: four times as many rows */
	if (window_covers(w, relid, cid, version, generation, rowid - 1) && w->last == rowid - 1)
		rows = (int) Min((uint64) most, (w->last - w->first + 1) * 4);
	else
		rows = Min(most, FETCH_WINDOW_FIRST);
	window_take(w, rel, snapshot, version, generation, rowid, rows, narrow);
	return w;
}

/* The window's row of the id into the slot; false when the snapshot sees none */
static bool
window_store(FetchWindow *w, Relation rel, uint64 rowid, TupleTableSlot *slot)
{
	int64		row = window_row(w, rowid);
	MemoryContext old;

	if (row < 0)
		return false;
	w->r.rel = rel;
	MemoryContextReset(w->rowcxt);
	old = MemoryContextSwitchTo(w->rowcxt);
	lt_reader_store_row(&w->r, &w->batch, row, slot);
	MemoryContextSwitchTo(old);
	slot->tts_tableOid = RelationGetRelid(rel);
	/* the values must outlive the window */
	ExecMaterializeSlot(slot);
	return true;
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
	LtBatch		batch;
	uint64		version;
	bool		any = false;
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

	version = lt_snapshot_version(rel, lt_table(rel), snapshot);
	if (n == 1)
	{
		int			kind = snapshot == SnapshotAny ? WINDOW_ANY : WINDOW_SNAPSHOT;
		FetchWindow *w = window_for(&windows[kind], rel, snapshot, version, rowids[0], false);

		if (w != NULL)
		{
			found[0] = window_store(w, rel, rowids[0], slots[0]);
			return found[0];
		}
	}

	cxt = AllocSetContextCreate(CurrentMemoryContext, "lance_tam fetch", ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(cxt);
	{
		uint64	   *want = palloc(sizeof(uint64) * missing);
		int			k = 0;

		for (int i = 0; i < n; i++)
			if (!found[i])
				want[k++] = rowids[i];
		take_rows(rel, snapshot, version, false, want, k, &r, &batch);
	}
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

/*
 * Whether the snapshot sees the row, as a fetch of it would find it, from
 * the rows' ids alone: tuple_satisfies_snapshot's, which a foreign key's
 * check asks by SnapshotSelf of each row its AFTER trigger fetched by
 * SnapshotAny.  Of a table the transaction has only appended to, the two
 * see the same rows at one version, and SnapshotAny's window answers; else
 * a run of checks takes windows of the narrowest column, and a check
 * alone a take of the row's.
 */
bool
lt_fetch_visible(Relation rel, Snapshot snapshot, uint64 rowid)
{
	Oid			relid = RelationGetRelid(rel);
	uint64		version = lt_snapshot_version(rel, lt_table(rel), snapshot);
	int64		cid = snapshot_cid(snapshot);
	uint64		generation = lt_xact_generation(relid);
	LtReader   *reader = NULL;
	FetchWindow *w;
	MemoryContext cxt;
	MemoryContext old;
	LtReader	r;
	LtBatch		batch;
	bool		visible;

	if (reader_row(rel, snapshot, rowid, &reader) >= 0)
		return true;
	/* the window of fetches by the snapshot's command */
	for (int i = WINDOW_ANY; i <= WINDOW_SNAPSHOT; i++)
		if (window_covers(&windows[i], relid, cid, version, generation, rowid))
			return window_row(&windows[i], rowid) >= 0;
	if (cid == PG_INT64_MAX && lt_xact_only_appended(relid) &&
		window_covers(&windows[WINDOW_ANY], relid, -1, version, generation, rowid))
		return window_row(&windows[WINDOW_ANY], rowid) >= 0;
	/* the checks' own window, which keeps up with their run */
	w = window_for(&windows[WINDOW_VISIBLE], rel, snapshot, version, rowid, true);
	if (w != NULL)
		return window_row(w, rowid) >= 0;

	cxt = AllocSetContextCreate(CurrentMemoryContext, "lance_tam fetch", ALLOCSET_SMALL_SIZES);
	old = MemoryContextSwitchTo(cxt);
	take_rows(rel, snapshot, version, true, &rowid, 1, &r, &batch);
	visible = batch.nrows > 0;
	lt_batch_release(&batch);
	MemoryContextSwitchTo(old);
	MemoryContextDelete(cxt);
	return visible;
}
