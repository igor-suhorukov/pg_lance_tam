/*-------------------------------------------------------------------------
 *
 * tableam.c
 *	  The lance table access method (docs/design.md §2): PostgreSQL 19's
 *	  TableAmRoutine over a Lance dataset.
 *
 *	  Scans read through scan.c's reader; a parallel scan's participants
 *	  share the version's fragments through an atomic counter in the DSM.
 *	  Writes go to xact.c, which commits them with the transaction.  A row's
 *	  TID is its row id (lance_tam.h), so TID scans, bitmap scans and the
 *	  indexes of PostgreSQL's own AMs work over the table.  An UPDATE that
 *	  changes no column an index reads keeps the row's TID, its new version
 *	  under the row's id (update_in_place()), so that no index needs an
 *	  entry for it; any other gives the row a new TID; there are no update
 *	  chains: a TID's version is the one the snapshot sees.  Rows are not
 *	  locked: a write that loses to a concurrent one fails at commit (§2.7).
 *
 *	  The relation's own storage is an empty main fork, made as heap makes
 *	  its own so that smgr and WAL see a relation like any other; the
 *	  table's data is its dataset's.  Making the relation makes the dataset
 *	  -- or attaches one that is at the table's uri -- and the catalog row.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_trigger.h"
#include "common/pg_prng.h"
#include "catalog/pg_am.h"
#include "catalog/pg_am_d.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/defrem.h"
#include "commands/progress.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/tidbitmap.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "storage/read_stream.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/tuplesort.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "lance_tam.h"

static const TableAmRoutine lt_routine;

const TableAmRoutine *
lt_tableam(void)
{
	return &lt_routine;
}

bool
lt_is_lance_rel(Relation rel)
{
	return rel->rd_tableam == &lt_routine;
}

Oid
lt_am_oid(void)
{
	static Oid	oid = InvalidOid;

	if (!OidIsValid(oid))
		oid = get_table_am_oid("lance", true);
	return oid;
}

bool
lt_is_lance_relid(Oid relid)
{
	HeapTuple	tup;
	bool		result = false;
	Oid			am = lt_am_oid();

	if (!OidIsValid(am))
		return false;
	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_class form = (Form_pg_class) GETSTRUCT(tup);

		result = form->relam == am &&
			(form->relkind == RELKIND_RELATION || form->relkind == RELKIND_MATVIEW);
		ReleaseSysCache(tup);
	}
	return result;
}

/* ------------------------------------------------------------------------
 * Scans
 * ------------------------------------------------------------------------
 */

typedef struct LtParallelScanDescData
{
	ParallelTableScanDescData base;
	pg_atomic_uint64 next_fragment; /* the fragments' index a participant
									 * takes next */
	pg_atomic_uint64 version;	/* the version every participant reads: the
								 * first one's, so that a table that follows
								 * its dataset is read at one version */
} LtParallelScanDescData;

typedef struct LtScanDescData
{
	TableScanDescData base;
	LtReader   *reader;
	bool		started;
	/* a parallel scan: the version's fragments, one taken at a time */
	uint64	   *fragments;
	int64		nfragments;
	bool		fragments_loaded;
	uint64		version;
	/* a TID range */
	uint64		min_rowid;
	uint64		max_rowid;
	/* a bitmap scan's rows, fetched a batch of TIDs at a time */
	TupleTableSlot **bm_slots;
	bool	   *bm_found;
	uint64	   *bm_rowids;
	int			bm_n;
	int			bm_pos;
	bool		bm_recheck;
	bool		bm_done;
	/* ANALYZE's sample, and a sample scan's blocks */
	LtBatch		sample;
	int64		sample_row;
	bool		sample_taken;	/* the sample row of the block given out */
	BlockNumber *sample_blocks;	/* a sample row's block, and its rows */
	int		   *sample_block_rows;
	int64		sample_rows_per_block;
	bool		sample_all;
	BlockNumber sample_block;
	int			sample_block_ntuples;
	TupleTableSlot *sample_slots[LT_ROWS_PER_BLOCK];
	LtLayout   *layout;
	MemoryContext cxt;
} LtScanDescData;

typedef LtScanDescData *LtScanDesc;

#define BITMAP_FETCH_ROWS	1024

static TableScanDesc
lt_scan_begin(Relation rel, Snapshot snapshot, int nkeys, ScanKeyData *key,
			  ParallelTableScanDesc pscan, uint32 flags)
{
	LtScanDesc	scan = palloc0(sizeof(LtScanDescData));

	scan->base.rs_rd = rel;
	scan->base.rs_snapshot = snapshot;
	scan->base.rs_nkeys = nkeys;
	scan->base.rs_key = nkeys > 0 ? key : NULL;
	scan->base.rs_flags = flags;
	scan->base.rs_parallel = pscan;
	scan->base.rs_instrument = NULL;
	scan->min_rowid = 0;
	scan->max_rowid = PG_UINT64_MAX;
	scan->cxt = CurrentMemoryContext;
	if (pscan && !IsParallelWorker())
	{
		LtXactTable *xt = lt_xact_table_by_relid(RelationGetRelid(rel));

		if (xt && xt->write)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot scan lance table \"%s\" in parallel after writing it in the same transaction",
							RelationGetRelationName(rel)),
					 errhint("SET max_parallel_workers_per_gather = 0 for the transaction.")));
	}
	pgstat_count_heap_scan(rel);
	return (TableScanDesc) scan;
}

static void
lt_scan_end_reader(LtScanDesc scan)
{
	if (scan->reader)
		lt_reader_end(scan->reader);
	scan->reader = NULL;
	scan->started = false;
}

static void
lt_scan_end(TableScanDesc sscan)
{
	LtScanDesc	scan = (LtScanDesc) sscan;

	lt_scan_end_reader(scan);
	lt_batch_release(&scan->sample);
	if (scan->bm_slots)
	{
		for (int i = 0; i < BITMAP_FETCH_ROWS; i++)
			if (scan->bm_slots[i])
				ExecDropSingleTupleTableSlot(scan->bm_slots[i]);
	}
	for (int i = 0; i < LT_ROWS_PER_BLOCK; i++)
		if (scan->sample_slots[i])
			ExecDropSingleTupleTableSlot(scan->sample_slots[i]);
	if (scan->base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->base.rs_snapshot);
	if (scan->fragments)
		pfree(scan->fragments);
	pfree(scan);
}

static void
lt_scan_rescan(TableScanDesc sscan, ScanKeyData *key, bool set_params, bool allow_strat,
			   bool allow_sync, bool allow_pagemode)
{
	LtScanDesc	scan = (LtScanDesc) sscan;

	if (key && scan->base.rs_nkeys > 0)
		memcpy(scan->base.rs_key, key, scan->base.rs_nkeys * sizeof(ScanKeyData));
	lt_scan_end_reader(scan);
	scan->bm_n = scan->bm_pos = 0;
	scan->bm_done = false;
}

/* The version's fragments, for a parallel participant to take from */
static void
load_fragments(LtScanDesc scan)
{
	LtTable    *t;
	uint64		version;
	LeDataset  *ds;
	uint64	   *ids = NULL;
	uint64	   *rows = NULL;
	int64		n = 0;
	LeError		err = {0};
	int			rc;

	if (scan->fragments_loaded)
		return;
	t = lt_table(scan->base.rs_rd);
	version = lt_snapshot_version(scan->base.rs_rd, t, scan->base.rs_snapshot);
	if (scan->base.rs_parallel)
	{
		LtParallelScanDescData *p = (LtParallelScanDescData *) scan->base.rs_parallel;
		uint64		first = 0;

		if (!pg_atomic_compare_exchange_u64(&p->version, &first, version))
			version = first;
	}
	scan->version = version;
	ds = lt_open_dataset(t, version);
	rc = lt_api()->fragment_list(ds, &ids, &rows, &n, &err);
	lt_api()->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "listing a lance table's fragments");
	scan->fragments = MemoryContextAlloc(scan->cxt, sizeof(uint64) * Max(n, 1));
	memcpy(scan->fragments, ids, sizeof(uint64) * n);
	scan->nfragments = n;
	lt_api()->free_u64(ids, n);
	lt_api()->free_u64(rows, n);
	scan->fragments_loaded = true;
}

/* Begin the reader: the whole version, or the next fragment of a parallel scan */
static bool
lt_scan_start(LtScanDesc scan)
{
	LtReadSpec	spec;
	MemoryContext old = MemoryContextSwitchTo(scan->cxt);

	memset(&spec, 0, sizeof(spec));
	spec.limit = -1;
	spec.with_rowid = true;
	if (scan->base.rs_parallel)
	{
		LtParallelScanDescData *p = (LtParallelScanDescData *) scan->base.rs_parallel;
		uint64		i;

		load_fragments(scan);
		i = pg_atomic_fetch_add_u64(&p->next_fragment, 1);
		if (i >= (uint64) scan->nfragments)
		{
			MemoryContextSwitchTo(old);
			return false;
		}
		spec.fragments = list_make1_int((int) scan->fragments[i]);
		spec.version = scan->version;
	}
	scan->reader = lt_reader_begin(scan->base.rs_rd, scan->base.rs_snapshot, &spec);
	scan->started = true;
	MemoryContextSwitchTo(old);
	return true;
}

/*
 * For vexec's source: a reader of the scan's next unit -- the whole table,
 * or a parallel participant's next fragment -- or NULL when none is left.
 */
LtReader *
lt_scan_take_reader(TableScanDesc sscan, LtReadSpec *base)
{
	LtScanDesc	scan = (LtScanDesc) sscan;
	LtReadSpec	spec = *base;

	if (scan->base.rs_parallel)
	{
		LtParallelScanDescData *p = (LtParallelScanDescData *) scan->base.rs_parallel;
		uint64		i;

		load_fragments(scan);
		i = pg_atomic_fetch_add_u64(&p->next_fragment, 1);
		if (i >= (uint64) scan->nfragments)
			return NULL;
		spec.fragments = list_make1_int((int) scan->fragments[i]);
		spec.version = scan->version;
	}
	else
	{
		if (scan->started)
			return NULL;
		scan->started = true;
	}
	return lt_reader_begin(sscan->rs_rd, sscan->rs_snapshot, &spec);
}

void
lt_scan_reset_take(TableScanDesc sscan)
{
	LtScanDesc	scan = (LtScanDesc) sscan;

	lt_scan_end_reader(scan);
}

/* A scan's keys, tested as heap tests them (HeapKeyTest) */
static bool
keys_match(TableScanDesc sscan, TupleTableSlot *slot)
{
	for (int i = 0; i < sscan->rs_nkeys; i++)
	{
		ScanKey		key = &sscan->rs_key[i];
		bool		isnull;
		Datum		d;

		if (key->sk_flags & SK_ISNULL)
			return false;
		d = slot_getattr(slot, key->sk_attno, &isnull);
		if (isnull)
			return false;
		if (!DatumGetBool(FunctionCall2Coll(&key->sk_func, key->sk_collation, d, key->sk_argument)))
			return false;
	}
	return true;
}

static bool
lt_scan_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	LtScanDesc	scan = (LtScanDesc) sscan;

	if (ScanDirectionIsBackward(direction))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lance tables cannot be scanned backward"),
				 errhint("Declare the cursor without SCROLL, or add an ORDER BY.")));
	for (;;)
	{
		if (!scan->started && !lt_scan_start(scan))
		{
			ExecClearTuple(slot);
			return false;
		}
		while (lt_reader_next(scan->reader, slot))
		{
			if (sscan->rs_nkeys > 0 && !keys_match(sscan, slot))
				continue;
			pgstat_count_heap_getnext(sscan->rs_rd);
			return true;
		}
		/* a parallel participant takes the next fragment */
		lt_scan_end_reader(scan);
		if (!scan->base.rs_parallel)
		{
			scan->started = true;	/* the scan is done */
			scan->reader = NULL;
			ExecClearTuple(slot);
			return false;
		}
	}
}

/* ------------------------------------------------------------------------
 * TID ranges
 * ------------------------------------------------------------------------
 */

static void
lt_scan_set_tidrange(TableScanDesc sscan, ItemPointer mintid, ItemPointer maxtid)
{
	LtScanDesc	scan = (LtScanDesc) sscan;

	scan->min_rowid = ((uint64) ItemPointerGetBlockNumberNoCheck(mintid) << 8);
	if (ItemPointerGetOffsetNumberNoCheck(mintid) > 0)
		scan->min_rowid += ItemPointerGetOffsetNumberNoCheck(mintid) - 1;
	scan->max_rowid = ((uint64) ItemPointerGetBlockNumberNoCheck(maxtid) << 8);
	if (ItemPointerGetOffsetNumberNoCheck(maxtid) > 0)
		scan->max_rowid += ItemPointerGetOffsetNumberNoCheck(maxtid) - 1;
	else if (scan->max_rowid > 0)
		scan->max_rowid--;
	lt_scan_end_reader(scan);
}

static bool
lt_scan_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	LtScanDesc	scan = (LtScanDesc) sscan;

	while (lt_scan_getnextslot(sscan, direction, slot))
	{
		uint64		rowid = lt_tid_to_rowid(&slot->tts_tid);

		if (rowid >= scan->min_rowid && rowid <= scan->max_rowid)
			return true;
	}
	return false;
}

/* ------------------------------------------------------------------------
 * Parallel scans
 * ------------------------------------------------------------------------
 */

static Size
lt_parallelscan_estimate(Relation rel)
{
	return sizeof(LtParallelScanDescData);
}

static Size
lt_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	LtParallelScanDescData *p = (LtParallelScanDescData *) pscan;

	p->base.phs_locator = rel->rd_locator;
	p->base.phs_syncscan = false;
	pg_atomic_init_u64(&p->next_fragment, 0);
	pg_atomic_init_u64(&p->version, 0);
	return sizeof(LtParallelScanDescData);
}

static void
lt_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	LtParallelScanDescData *p = (LtParallelScanDescData *) pscan;

	pg_atomic_write_u64(&p->next_fragment, 0);
}

/* ------------------------------------------------------------------------
 * Fetches by TID
 * ------------------------------------------------------------------------
 */

static IndexFetchTableData *
lt_index_fetch_begin(Relation rel, uint32 flags)
{
	IndexFetchTableData *scan = palloc0(sizeof(IndexFetchTableData));

	scan->rel = rel;
	scan->flags = flags;
	return scan;
}

static void
lt_index_fetch_reset(IndexFetchTableData *scan)
{
}

static void
lt_index_fetch_end(IndexFetchTableData *scan)
{
	pfree(scan);
}

/*
 * The row the backend fetched by TID last, and whether by a snapshot that
 * sees its newest version (dirty, self, any) or by an MVCC one: of a row
 * updated in place, a TID's version is the one its fetch found, as heap's
 * TID names the version its index probe found (lt_tuple_lock()).
 */
static Oid	last_fetch_relid = InvalidOid;
static uint64 last_fetch_rowid = 0;
static bool last_fetch_newest = false;

static bool
fetch_one(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	uint64		rowid;
	bool		found = false;

	last_fetch_relid = RelationGetRelid(rel);
	last_fetch_rowid = lt_tid_valid(tid) ? lt_tid_to_rowid(tid) : 0;
	last_fetch_newest = snapshot == NULL || !IsMVCCSnapshot(snapshot);

	/*
	 * A dirty snapshot reports the transactions a row waits on, which the
	 * AM sets as heap's HeapTupleSatisfiesDirty() does: none here, since a
	 * writer of a table with a unique index writes alone (xact.c).
	 */
	if (snapshot && snapshot->snapshot_type == SNAPSHOT_DIRTY)
	{
		snapshot->xmin = InvalidTransactionId;
		snapshot->xmax = InvalidTransactionId;
		snapshot->speculativeToken = 0;
	}
	if (!lt_tid_valid(tid))
		return false;
	rowid = lt_tid_to_rowid(tid);
	lt_fetch_rows(rel, snapshot, &rowid, 1, &slot, &found);
	if (!found)
		ExecClearTuple(slot);
	return found;
}

static bool
lt_index_fetch_tuple(IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot,
					 TupleTableSlot *slot, bool *call_again, bool *all_dead)
{
	*call_again = false;
	if (all_dead)
		*all_dead = false;
	if (!fetch_one(scan->rel, tid, snapshot, slot))
		return false;
	pgstat_count_heap_fetch(scan->rel);
	return true;
}

static bool
lt_tuple_fetch_row_version(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot)
{
	return fetch_one(rel, tid, snapshot, slot);
}

static bool
lt_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return lt_tid_valid(tid);
}

static void
lt_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	/*
	 * A row updated in place keeps its TID; one updated otherwise is a new
	 * row: a TID is its own latest version either way
	 */
}

static bool
lt_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	TupleTableSlot *probe = MakeSingleTupleTableSlot(RelationGetDescr(rel), lt_slot_ops_of());
	bool		found = fetch_one(rel, &slot->tts_tid, snapshot, probe);

	ExecDropSingleTupleTableSlot(probe);
	return found;
}

/*
 * An index's entries the table knows dead: none -- a fetch never reports a
 * row dead to every snapshot, so the index marks no entry dead of its own
 * either.  The array keeps the deletable entries alone, as callers rely on
 * (heapam_handler.c's heap_index_delete_tuples).
 */
static TransactionId
lt_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	int			n = 0;

	for (int i = 0; i < delstate->ndeltids; i++)
	{
		TM_IndexDelete *d = &delstate->deltids[i];

		if (delstate->status[d->id].knowndeletable)
			delstate->deltids[n++] = *d;
	}
	delstate->ndeltids = n;
	return InvalidTransactionId;
}

/* ------------------------------------------------------------------------
 * Writes
 * ------------------------------------------------------------------------
 */

static void
lt_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, uint32 options,
				BulkInsertStateData *bistate)
{
	lt_xact_insert(rel, slot, cid);
	pgstat_count_heap_insert(rel, 1);
}

static void
lt_tuple_insert_speculative(Relation rel, TupleTableSlot *slot, CommandId cid, uint32 options,
							BulkInsertStateData *bistate, uint32 specToken)
{
	lt_xact_insert(rel, slot, cid);
	pgstat_count_heap_insert(rel, 1);
}

static void
lt_tuple_complete_speculative(Relation rel, TupleTableSlot *slot, uint32 specToken, bool succeeded)
{
	/* a conflict found after the insert: the row goes again */
	if (!succeeded)
		lt_xact_delete(rel, lt_tid_to_rowid(&slot->tts_tid), GetCurrentCommandId(true), 0);
}

static void
lt_multi_insert(Relation rel, TupleTableSlot **slots, int nslots, CommandId cid, uint32 options,
				BulkInsertStateData *bistate)
{
	for (int i = 0; i < nslots; i++)
		lt_xact_insert(rel, slots[i], cid);
	pgstat_count_heap_insert(rel, nslots);
}

static TM_Result
self_modified(Relation rel, ItemPointer tid, CommandId cid, TM_FailureData *tmfd)
{
	CommandId	dcid;

	if (lt_xact_row_state(rel, lt_tid_to_rowid(tid), cid, &dcid) == LE_WRITE_ROW_DELETED)
	{
		tmfd->ctid = *tid;
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = dcid;
		tmfd->traversed = false;
		return TM_SelfModified;
	}
	return TM_Ok;
}

static TM_Result
lt_tuple_delete(Relation rel, ItemPointer tid, CommandId cid, uint32 options, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *tmfd)
{
	TM_Result	r = self_modified(rel, tid, cid, tmfd);

	if (r != TM_Ok)
		return r;
	lt_xact_delete(rel, lt_tid_to_rowid(tid), cid, lt_xact_read_version(rel, snapshot));
	pgstat_count_heap_delete(rel);
	return TM_Ok;
}

/* Whether a column has the same value in two rows, as heap_attr_equals has it */
static bool
attr_equal(TupleDesc desc, int attno, TupleTableSlot *a, TupleTableSlot *b)
{
	Form_pg_attribute att = TupleDescAttr(desc, attno - 1);

	if (a->tts_isnull[attno - 1] || b->tts_isnull[attno - 1])
		return a->tts_isnull[attno - 1] == b->tts_isnull[attno - 1];
	return datum_image_eq(a->tts_values[attno - 1], b->tts_values[attno - 1], att->attbyval,
						  att->attlen);
}

/*
 * Whether an UPDATE keeps the row's TID, its new version written under the
 * row's id: as pg_inplace_update_am decides an update in place (its
 * DESIGN.md §7.4), so that, as heap's HOT update does, it leaves every
 * index alone -- and an index scan that runs the UPDATE from its operator,
 * its page locked, does not wait for the page itself.  No column an index
 * reads may change (lt_table_inplace_blocking), and no trigger may need the
 * row's OLD and NEW versions by their TIDs, which are then one: a user's
 * AFTER ROW UPDATE trigger, a foreign key's on the referenced side whose
 * check the update makes needed, an OLD transition table.  A foreign key's
 * check on the referencing side reads NEW alone.
 */
static bool
update_in_place(Relation rel, ItemPointer otid, TupleTableSlot *newslot)
{
	TriggerDesc *trigdesc = rel->trigdesc;
	TupleDesc	desc = RelationGetDescr(rel);
	Bitmapset  *blocking;
	TupleTableSlot *oldslot;
	bool		pk_checks = false;
	bool		keep = true;
	int			k = -1;

	if (!lt_update_in_place)
		return false;
	if (trigdesc && trigdesc->trig_update_old_table)
		return false;
	if (trigdesc && trigdesc->trig_update_after_row)
	{
		for (int i = 0; i < trigdesc->numtriggers; i++)
		{
			Trigger    *trig = &trigdesc->triggers[i];

			if (!TRIGGER_TYPE_MATCHES(trig->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER,
									  TRIGGER_TYPE_UPDATE) ||
				trig->tgenabled == TRIGGER_DISABLED)
				continue;
			switch (RI_FKey_trigger_type(trig->tgfoid))
			{
				case RI_TRIGGER_FK:
					break;
				case RI_TRIGGER_PK:
					pk_checks = true;
					break;
				default:
					return false;
			}
		}
	}
	blocking = lt_table_inplace_blocking(rel);
	if (blocking == NULL && !pk_checks)
		return true;

	/* the version updated, as the update's scan read it */
	oldslot = MakeSingleTupleTableSlot(desc, lt_slot_ops_of());
	if (!fetch_one(rel, otid, SnapshotAny, oldslot))
	{
		ExecDropSingleTupleTableSlot(oldslot);
		return false;
	}
	slot_getallattrs(oldslot);
	slot_getallattrs(newslot);
	while (keep && (k = bms_next_member(blocking, k)) >= 0)
	{
		AttrNumber	attno = k + FirstLowInvalidHeapAttributeNumber;

		if (attno > 0)
			keep = attr_equal(desc, attno, oldslot, newslot);
		else if (attno == InvalidAttrNumber)
		{
			/* an index's expression reads the whole row */
			for (int a = 1; keep && a <= desc->natts; a++)
				if (!TupleDescAttr(desc, a - 1)->attisdropped)
					keep = attr_equal(desc, a, oldslot, newslot);
		}
	}
	for (int i = 0; keep && pk_checks && i < trigdesc->numtriggers; i++)
	{
		Trigger    *trig = &trigdesc->triggers[i];

		if (TRIGGER_TYPE_MATCHES(trig->tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER,
								 TRIGGER_TYPE_UPDATE) &&
			trig->tgenabled != TRIGGER_DISABLED &&
			RI_FKey_trigger_type(trig->tgfoid) == RI_TRIGGER_PK &&
			RI_FKey_pk_upd_check_required(trig, rel, oldslot, newslot))
			keep = false;
	}
	ExecDropSingleTupleTableSlot(oldslot);
	return keep;
}

static TM_Result
lt_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot, CommandId cid, uint32 options,
				Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
				LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	TM_Result	r = self_modified(rel, otid, cid, tmfd);
	uint64		rowid = lt_tid_to_rowid(otid);
	bool		in_place;

	*lockmode = LockTupleExclusive;
	if (r != TM_Ok)
	{
		*update_indexes = TU_None;
		return r;
	}
	in_place = update_in_place(rel, otid, slot);
	lt_xact_delete(rel, rowid, cid, lt_xact_read_version(rel, snapshot));
	if (in_place)
	{
		/* the row keeps its TID: no index needs an entry */
		lt_xact_insert_version(rel, slot, cid, rowid);
		*update_indexes = TU_None;
	}
	else
	{
		lt_xact_insert(rel, slot, cid);
		*update_indexes = TU_All;
	}
	pgstat_count_heap_update(rel, in_place, false);
	return TM_Ok;
}

/*
 * A row is not locked: its writers are told apart at commit.  The lock
 * fetches the row as the snapshot sees it, as heap's does.
 */
static TM_Result
lt_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
			  CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags,
			  TM_FailureData *tmfd)
{
	TM_Result	r = self_modified(rel, tid, cid, tmfd);

	if (r != TM_Ok)
	{
		/*
		 * A row the command updated in place, found by a probe that sees
		 * the newest version (ON CONFLICT's, by a dirty snapshot): the TID
		 * names the version the update made, which the command does not
		 * see, as heap's lock of that version finds it invisible -- and ON
		 * CONFLICT DO UPDATE refuses to change the row again.  Found by an
		 * MVCC snapshot (a foreign key's check, whose operator updated the
		 * row), the TID names the version the snapshot read, which heap's
		 * lock reports TM_SelfModified.
		 */
		if (r == TM_SelfModified && tmfd->cmax == cid && last_fetch_newest &&
			last_fetch_relid == RelationGetRelid(rel) &&
			last_fetch_rowid == lt_tid_to_rowid(tid) && fetch_one(rel, tid, SnapshotSelf, slot))
			return TM_Invisible;
		return r;
	}
	if (!fetch_one(rel, tid, snapshot, slot))
	{
		tmfd->ctid = *tid;
		tmfd->xmax = InvalidTransactionId;
		tmfd->cmax = InvalidCommandId;
		tmfd->traversed = false;

		/*
		 * A row this transaction inserted in the running command is
		 * invisible to the command, as heap reports it: ON CONFLICT DO
		 * UPDATE then refuses to change it a second time, where a deleted
		 * row would have it try again.
		 */
		if (fetch_one(rel, tid, SnapshotSelf, slot))
			return TM_Invisible;
		return TM_Deleted;
	}
	tmfd->traversed = false;
	return TM_Ok;
}

static void
lt_finish_bulk_insert(Relation rel, uint32 options)
{
	lt_xact_flush_rel(rel);
}

/* ------------------------------------------------------------------------
 * DDL
 * ------------------------------------------------------------------------
 */

static char *
sanitize(const char *s)
{
	char	   *out = pstrdup(s);

	for (char *p = out; *p; p++)
		if (!(isalnum((unsigned char) *p) || *p == '_' || *p == '-' || *p == '.'))
			*p = '_';
	return out;
}

/* Where a table's dataset goes when its options name no uri */
static char *
default_uri(Relation rel)
{
	char	   *base;
	size_t		len;

	if (lt_default_location && lt_default_location[0])
		base = pstrdup(lt_default_location);
	else
		base = psprintf("%s/pg_lance", DataDir);
	len = strlen(base);
	while (len > 1 && base[len - 1] == '/')
		base[--len] = '\0';
	return psprintf("%s/%s/%s/%s_%u.lance", base,
					sanitize(get_database_name(MyDatabaseId)),
					sanitize(get_namespace_name(RelationGetNamespace(rel))),
					sanitize(RelationGetRelationName(rel)),
					RelationGetRelid(rel));
}

static List *
attribute_names(TupleDesc desc)
{
	List	   *names = NIL;

	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);

		/* a virtual generated column is PostgreSQL's to compute, not stored */
		names = lappend(names, att->attisdropped || att->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL ?
						pstrdup("") : pstrdup(NameStr(att->attname)));
	}
	return names;
}

/* Every column the table names must be a column of the dataset */
static void
check_attached_columns(Relation rel, struct ArrowSchema *schema, const char *uri)
{
	TupleDesc	desc = RelationGetDescr(rel);

	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		bool		found = false;

		if (att->attisdropped || att->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
			continue;
		for (int64 j = 0; j < schema->n_children; j++)
			if (strcmp(schema->children[j]->name, NameStr(att->attname)) == 0)
				found = true;
		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" is not a column of the dataset at \"%s\"",
							NameStr(att->attname), uri)));
	}
}

/*
 * A new lance table: its dataset made at its uri, or attached when one is
 * there already, and its catalog row.
 */
static void
create_table_dataset(Relation rel)
{
	List	   *options = lt_pending_create_options();
	char	   *uri = lt_option_string(options, "uri");
	char	   *server = lt_option_string(options, "server");
	bool		managed;
	bool		read_only = lt_option_bool(options, "read_only", false);
	bool		follow = lt_option_bool(options, "follow", false);
	char	   *json;
	const LeApi *api = lt_api();
	LeDataset  *ds = NULL;
	LeError		err = {0};
	uint64		version = 0;
	bool		writable;
	int			rc;

	if (server == NULL && lt_default_server && lt_default_server[0])
		server = pstrdup(lt_default_server);
	if (strncmp(RelationGetRelationName(rel), "pg_temp_", 8) == 0 &&
		RelationGetNamespace(rel) != PG_TOAST_NAMESPACE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lance tables cannot be rewritten"),
				 errdetail("A rewrite would move the table's rows to a new dataset apart from the table's."),
				 errhint("Plain VACUUM compacts a lance table's dataset.  ALTER TABLE keeps it for a new column's constant default and for a type change that converts no rows.")));
	if (follow && !read_only)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"follow\" needs \"read_only\"")));

	managed = lt_option_bool(options, "managed", uri == NULL);
	if (uri == NULL)
		uri = default_uri(rel);
	json = lt_storage_json(options, server, rel->rd_rel->relowner);

	rc = api->dataset_open(uri, json, -1, &ds, &err);
	if (rc == LE_OK)
	{
		/* a dataset is there: attach it */
		LeDatasetInfo info;
		struct ArrowSchema schema;

		memset(&schema, 0, sizeof(schema));
		memset(&info, 0, sizeof(info));
		PG_TRY();
		{
			if (api->dataset_info(ds, &info, &err) != LE_OK)
				lt_raise(&err, "reading the dataset at the table's uri");
			if (api->dataset_schema(ds, &schema, &err) != LE_OK)
				lt_raise(&err, "reading the dataset's schema");
			check_attached_columns(rel, &schema, uri);
		}
		PG_FINALLY();
		{
			if (schema.release)
				schema.release(&schema);
			api->dataset_close(ds);
		}
		PG_END_TRY();
		version = info.version;
		writable = info.stable_row_ids && !read_only;
		if (!info.stable_row_ids && !read_only)
			ereport(NOTICE,
					(errmsg("the dataset at \"%s\" has no stable row ids: the table is read-only", uri)));
		if (lt_option_bool(options, "managed", false))
			ereport(NOTICE,
					(errmsg("the dataset at \"%s\" exists: dropping the table will remove it", uri)));
		else
			managed = false;
	}
	else if (rc == LE_ERR_NOT_FOUND)
	{
		/* no dataset: make an empty one of the table's columns */
		LtLayout   *layout;
		struct ArrowSchema schema;
		int		   *attidx;
		int			n = 0;
		LeWriteSpec spec;
		TupleDesc	desc = RelationGetDescr(rel);
		char	   *props;

		lt_take_string(err.message);
		err.message = NULL;
		if (read_only)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("no Lance dataset at \"%s\" to attach read-only", uri)));
		layout = lt_layout_build_desc(desc, NULL, 0, CurrentMemoryContext);
		attidx = palloc(sizeof(int) * Max(desc->natts, 1));
		for (int i = 0; i < desc->natts; i++)
			if (layout->cols[i].kind != LT_NONE)
				attidx[n++] = i;
		if (n == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("a new lance table needs at least one column")));
		memset(&schema, 0, sizeof(schema));
		lt_export_schema(layout, n, attidx, &schema);
		PG_TRY();
		{
			lt_schema_set_encoding(&schema, options, true);
		}
		PG_CATCH();
		{
			if (schema.release)
				schema.release(&schema);
			PG_RE_THROW();
		}
		PG_END_TRY();
		memset(&spec, 0, sizeof(spec));
		spec.size = sizeof(spec);
		spec.create = true;
		spec.stable_row_ids = true;
		spec.storage_version = lt_option_string(options, "storage_version");
		props = lt_xact_properties(RelationGetRelid(rel));
		rc = api->create_dataset(uri, json, &schema, &spec, props, &version, &err);
		if (schema.release)
			schema.release(&schema);
		if (rc != LE_OK)
			lt_raise(&err, "creating a lance table's dataset");
		writable = true;
		lt_xact_note_created(RelationGetRelid(rel), uri, json, managed);
	}
	else
		lt_raise(&err, "opening the dataset at the table's uri");

	lt_catalog_insert(RelationGetRelid(rel), uri, version, managed, writable, follow,
					  attribute_names(RelationGetDescr(rel)), options, server);
}

/*
 * A relation's new storage: an empty main fork, made as heap makes its own;
 * for a new table its dataset and catalog row, for a TRUNCATE of one an
 * empty version at commit.
 */
static void
lt_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrlocator, char persistence,
								TransactionId *freezeXid, MultiXactId *minmulti)
{
	SMgrRelation srel;

	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	srel = RelationCreateStorage(*newrlocator, persistence, true);
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}
	smgrclose(srel);

	if (lt_table_by_relid(RelationGetRelid(rel), true) != NULL)
	{
		LtXactTable *xt = lt_xact_table(rel, true);
		LeError		err = {0};

		lt_xact_write(rel, xt);
		lt_xact_flush(xt);
		if (lt_api()->write_truncate(xt->write, (int64) GetCurrentCommandId(true),
									 GetCurrentSubTransactionId(), &err) != LE_OK)
			lt_raise(&err, "truncating a lance table");
		xt->truncated = true;
		xt->rows_inserted = xt->rows_deleted = 0;
		return;
	}
	create_table_dataset(rel);
}

static void
lt_relation_nontransactional_truncate(Relation rel)
{
	LtXactTable *xt;
	LeError		err = {0};

	/* ON COMMIT DELETE ROWS, after the transaction's commit to Lance */
	if (lt_xact_committing())
	{
		lt_xact_truncate_committed(rel);
		return;
	}
	xt = lt_xact_table(rel, true);
	lt_xact_write(rel, xt);
	lt_xact_flush(xt);
	if (lt_api()->write_truncate(xt->write, (int64) GetCurrentCommandId(true),
								 GetCurrentSubTransactionId(), &err) != LE_OK)
		lt_raise(&err, "truncating a lance table");
	xt->truncated = true;
	xt->rows_inserted = xt->rows_deleted = 0;
}

/* SET TABLESPACE: the empty main fork at its new place; the dataset stays */
static void
lt_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	SMgrRelation srel = RelationCreateStorage(*newrlocator, rel->rd_rel->relpersistence, true);

	smgrclose(srel);
}

static void
lt_relation_copy_for_cluster(Relation OldTable, Relation NewTable, Relation OldIndex, bool use_sort,
							 TransactionId OldestXmin, Snapshot snapshot,
							 TransactionId *xid_cutoff, MultiXactId *multi_cutoff,
							 double *num_tuples, double *tups_vacuumed, double *tups_recently_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("lance tables cannot be rewritten by CLUSTER, REPACK or VACUUM FULL"),
			 errhint("Plain VACUUM compacts a lance table's dataset.")));
}

/*
 * What compaction may use, as PostgreSQL bounds VACUUM's parallelism and
 * memory: its rewrite tasks run at once one and the workers a parallel
 * VACUUM may have -- PARALLEL n, else max_parallel_maintenance_workers; for
 * autovacuum, a table's autovacuum_parallel_workers, else
 * autovacuum_max_parallel_workers, 0 by default -- and their scans read
 * ahead maintenance_work_mem (autovacuum's autovacuum_work_mem) between
 * them, in batches of lance.batch_rows rows.  nworkers is VacuumParams':
 * 0 by default, -1 for PARALLEL 0.  A JSON object of optimize()'s options.
 */
char *
lt_compaction_bounds(int nworkers)
{
	bool		autovacuum = AmAutoVacuumWorkerProcess();
	int			cap = autovacuum ? autovacuum_max_parallel_workers : max_parallel_maintenance_workers;
	int			mem_kb = autovacuum && autovacuum_work_mem != -1 ? autovacuum_work_mem : maintenance_work_mem;
	int			tasks = 1 + (nworkers < 0 ? 0 : nworkers > 0 ? Min(nworkers, cap) : cap);
	int64		io_buffer = Max((int64) mem_kb * 1024 / tasks, (int64) 1024 * 1024);

	return psprintf("{\"num_threads\": %d, \"io_buffer_size\": " INT64_FORMAT ", \"batch_size\": %d}",
					tasks, io_buffer, lt_batch_rows);
}

#define VACUUM_LOCK_WAIT_MS		5000
#define VACUUM_LOCK_INTERVAL_MS	50

/*
 * The table's commit lock, to the end of the transaction, for VACUUM.
 * Autovacuum takes it only as it comes free, trying for VACUUM_LOCK_WAIT_MS
 * as its truncation of a heap table tries for that table's lock, or not
 * at all without "wait": it neither keeps a writer of the table waiting --
 * which would cancel it -- nor waits long for one that holds the lock from
 * its first write, a writer of a table with a unique index.
 */
static bool
vacuum_lock_commit(Oid relid, bool wait)
{
	if (!AmAutoVacuumWorkerProcess())
	{
		lt_lock_table_commit(relid);
		return true;
	}
	for (int waited = 0;; waited += VACUUM_LOCK_INTERVAL_MS)
	{
		if (ConditionalLockDatabaseObject(lt_catalog_relid(), relid, 0, ExclusiveLock))
			return true;
		if (!wait || waited >= VACUUM_LOCK_WAIT_MS)
			return false;
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 VACUUM_LOCK_INTERVAL_MS, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * VACUUM's commit, under the table's commit lock: the compaction written on
 * an earlier version, which Lance refuses when a commit since then deleted
 * rows it rewrote -- it is then left for a later VACUUM -- then the
 * indices brought up to new rows and old versions removed, and the catalog
 * set to the version it leaves, whose manifest *info describes.  False when
 * autovacuum could not take the lock.
 */
static bool
vacuum_commit(Relation rel, LtTable *t, LeCompaction *c, int elevel, LeDatasetInfo *info)
{
	Oid			relid = RelationGetRelid(rel);
	const LeApi *api = lt_api();
	char	   *props;
	uint64		catalog_version;
	uint64		version;
	uint64		new_version = 0;
	LeDataset  *ds;
	LeDataset  *volatile last;
	LeError		err = {0};
	char	   *result = NULL;
	int			rc;

	if (!vacuum_lock_commit(relid, true))
		return false;
	props = lt_xact_properties(relid);
	catalog_version = lt_catalog_version(relid, NULL);
	version = lt_commit_base(relid, t->uri, t->storage_json, catalog_version, props);
	if (c != NULL)
	{
		ds = lt_open_dataset(t, version);
		/* the commit runs to its end, as a writer's does (xact.c) */
		HOLD_INTERRUPTS();
		rc = api->compact_commit(c, ds, &new_version, &result, &err);
		RESUME_INTERRUPTS();
		api->dataset_close(ds);
		if (rc == LE_OK)
		{
			version = new_version;
			result = lt_take_string(result);
			ereport(elevel,
					(errmsg("lance table \"%s\": %s", RelationGetRelationName(rel),
							result ? result : "")));
		}
		else if (rc == LE_ERR_CONFLICT)
		{
			ereport(elevel,
					(errmsg("lance table \"%s\": compaction left for a later VACUUM",
							RelationGetRelationName(rel)),
					 errdetail_internal("%s", err.message ? err.message : "")));
			api->free_string(err.message);
		}
		else
			lt_raise(&err, "committing a lance table's compaction");
	}

	last = lt_open_dataset(t, version);
	PG_TRY();
	{
		if (api->optimize(last, "{\"compact\": false}", props, &new_version, &result, &err) != LE_OK)
			lt_raise(&err, "optimizing a lance table's indices");
		result = lt_take_string(result);
		if (new_version != version)
		{
			version = new_version;
			ereport(elevel,
					(errmsg("lance table \"%s\": %s", RelationGetRelationName(rel),
							result ? result : "")));
			ds = last;
			last = NULL;
			api->dataset_close(ds);
			last = lt_open_dataset(t, version);
		}
		if (lt_version_retention >= 0)
		{
			char	   *cleaned = NULL;

			if (api->cleanup(last, lt_version_retention, &cleaned, &err) != LE_OK)
				lt_warn(&err, "could not remove a lance table's old versions");
			else
			{
				cleaned = lt_take_string(cleaned);
				ereport(elevel,
						(errmsg("lance table \"%s\" old versions: %s",
								RelationGetRelationName(rel), cleaned ? cleaned : "")));
			}
		}
		memset(info, 0, sizeof(*info));
		if (api->dataset_info(last, info, &err) != LE_OK)
			lt_raise(&err, "reading a lance table's dataset");
	}
	PG_FINALLY();
	{
		if (last)
			api->dataset_close(last);
	}
	PG_END_TRY();
	if (version != catalog_version)
		lt_catalog_set_version(relid, version);
	return true;
}

/*
 * VACUUM: Lance's maintenance of the dataset -- its small fragments
 * compacted and deletions materialized, its indices brought up to its new
 * rows, old versions removed -- and the table's statistics, as heap's
 * VACUUM leaves them: autovacuum then counts the table's changes from this
 * VACUUM on.  Compaction writes its new fragments on the catalog's version
 * without the table's commit lock, as many tasks at once as
 * lt_compaction_bounds() lets it, so that the table's writers keep
 * committing while it rewrites; its commit takes the lock (vacuum_commit).
 */
static void
lt_relation_vacuum(Relation rel, const VacuumParams *params, BufferAccessStrategy bstrategy)
{
	Oid			relid = RelationGetRelid(rel);
	int			elevel = (params->options & VACOPT_VERBOSE) ? INFO : DEBUG1;
	TimestampTz starttime = GetCurrentTimestamp();
	LtTable    *t = lt_table(rel);
	const LeApi *api;
	LeDataset  *ds;
	LeDatasetInfo info;
	LeCompaction *c = NULL;
	LeError		err = {0};
	bool		done = true;

	if (!t->writable)
		return;
	api = lt_api();
	memset(&info, 0, sizeof(info));
	if (lt_vacuum_compact)
	{
		/* autovacuum passes a table whose commit lock another holds */
		if (AmAutoVacuumWorkerProcess())
		{
			if (!vacuum_lock_commit(relid, false))
				return;
			UnlockDatabaseObject(lt_catalog_relid(), relid, 0, ExclusiveLock);
		}
		ds = lt_open_dataset(t, lt_catalog_version(relid, NULL));
		PG_TRY();
		{
			if (api->dataset_info(ds, &info, &err) != LE_OK)
				lt_raise(&err, "reading a lance table's dataset");
			if ((info.num_fragments > 1 || info.num_deleted_rows > 0) &&
				api->compact_prepare(ds, lt_compaction_bounds(params->nworkers), &c, &err) != LE_OK)
				lt_raise(&err, "compacting a lance table");
		}
		PG_FINALLY();
		{
			api->dataset_close(ds);
		}
		PG_END_TRY();

		/* the files a compaction did not commit are removed */
		PG_TRY();
		{
			done = vacuum_commit(rel, t, c, elevel, &info);
		}
		PG_FINALLY();
		{
			if (c)
				api->compact_free(c, true);
		}
		PG_END_TRY();
		if (!done)
		{
			ereport(elevel,
					(errmsg("lance table \"%s\": not vacuumed, its commit lock is taken",
							RelationGetRelationName(rel))));
			return;
		}
	}
	else
	{
		ds = lt_open_dataset(t, lt_catalog_version(relid, NULL));
		if (api->dataset_info(ds, &info, &err) != LE_OK)
		{
			api->dataset_close(ds);
			lt_raise(&err, "reading a lance table's dataset");
		}
		api->dataset_close(ds);
	}

	vac_update_relstats(rel,
						(BlockNumber) Min((info.num_rows + LT_ROWS_PER_BLOCK - 1) / LT_ROWS_PER_BLOCK,
										  (uint64) MaxBlockNumber),
						(double) info.num_rows, 0, 0, rel->rd_rel->relhasindex,
						InvalidTransactionId, InvalidMultiXactId, NULL, NULL, false);
	pgstat_report_vacuum(rel, (PgStat_Counter) info.num_rows,
						 (PgStat_Counter) info.num_deleted_rows, starttime);
}

/* ------------------------------------------------------------------------
 * ANALYZE: virtual blocks of LT_ROWS_PER_BLOCK rows
 * ------------------------------------------------------------------------
 */

static int
sample_columns(LtLayout *layout, int **attidx, LtColumn ***cols)
{
	int			n = 0;

	*attidx = palloc(sizeof(int) * Max(layout->natts, 1));
	*cols = palloc(sizeof(LtColumn *) * Max(layout->natts, 1));
	for (int i = 0; i < layout->natts; i++)
	{
		if (layout->cols[i].kind == LT_NONE)
			continue;
		(*attidx)[n] = i;
		(*cols)[n] = &layout->cols[i];
		n++;
	}
	return n;
}

/*
 * The first call drains the stream's block numbers: when it asks for every
 * block, the scan reads the table; else each block given yields one row at
 * a random position in it, counted as the block's rows.
 */
static bool
lt_scan_analyze_next_block(TableScanDesc sscan, ReadStream *stream)
{
	LtScanDesc	scan = (LtScanDesc) sscan;
	Relation	rel = sscan->rs_rd;

	if (scan->sample_rows_per_block == 0)
	{
		BufferAccessStrategy strategy;
		BlockNumber b;
		List	   *blocks = NIL;
		LtTable    *t = lt_table(rel);
		uint64		version = lt_snapshot_version(rel, t, sscan->rs_snapshot);
		LeDataset  *ds = lt_open_dataset(t, version);
		LeDatasetInfo info;
		LeError		err = {0};
		uint64		nblocks;

		memset(&info, 0, sizeof(info));
		if (lt_api()->dataset_info(ds, &info, &err) != LE_OK)
		{
			lt_api()->dataset_close(ds);
			lt_raise(&err, "reading a lance table's size");
		}
		nblocks = (info.num_rows + LT_ROWS_PER_BLOCK - 1) / LT_ROWS_PER_BLOCK;
		while ((b = read_stream_next_block(stream, &strategy)) != InvalidBlockNumber)
			blocks = lappend_int(blocks, (int) b);
		scan->sample_rows_per_block = LT_ROWS_PER_BLOCK;
		scan->sample_all = (uint64) list_length(blocks) >= nblocks;
		scan->layout = lt_layout_build(rel, scan->cxt);
		if (scan->sample_all)
		{
			lt_api()->dataset_close(ds);
			scan->sample_block = 0;
		}
		else
		{
			uint64	   *positions = palloc(sizeof(uint64) * Max(list_length(blocks), 1));
			int			n = 0;
			ListCell   *lc;
			pg_prng_state rng;
			LtReader	r;
			LeScanSpec	spec;
			struct ArrowSchema types;
			int			rc;

			pg_prng_seed(&rng, (uint64) random());
			scan->sample_blocks = MemoryContextAlloc(scan->cxt,
													 sizeof(BlockNumber) * Max(list_length(blocks), 1));
			scan->sample_block_rows = MemoryContextAlloc(scan->cxt,
														 sizeof(int) * Max(list_length(blocks), 1));
			foreach(lc, blocks)
			{
				uint64		first = (uint64) lfirst_int(lc) * LT_ROWS_PER_BLOCK;
				uint64		span;

				if (first >= info.num_rows)
					continue;
				span = Min((uint64) LT_ROWS_PER_BLOCK, info.num_rows - first);
				scan->sample_blocks[n] = (BlockNumber) lfirst_int(lc);
				scan->sample_block_rows[n] = (int) span;
				positions[n++] = first + pg_prng_uint64_range(&rng, 0, span - 1);
			}
			memset(&r, 0, sizeof(r));
			r.rel = rel;
			r.layout = scan->layout;
			r.ncols = sample_columns(scan->layout, &r.attidx, &r.cols);
			memset(&types, 0, sizeof(types));
			lt_reader_fill_spec(&r, NULL, &spec, &types);
			spec.write = NULL;
			memset(&scan->sample, 0, sizeof(scan->sample));
			rc = n == 0 ? LE_OK :
				lt_api()->take_positions(ds, &spec, positions, n, &scan->sample.array,
										 &scan->sample.schema, &err);
			if (types.release)
				types.release(&types);
			lt_api()->dataset_close(ds);
			if (rc != LE_OK)
				lt_raise(&err, "sampling a lance table");
			if (n > 0)
			{
				scan->sample.valid = true;
				lt_batch_bind(&scan->sample, r.ncols, r.cols);
			}
			scan->sample_row = -1;
		}
	}
	if (scan->sample_all)
	{
		/* the next run of rows of a scan of the whole table */
		LtReadSpec	spec;

		if (!scan->started)
		{
			MemoryContext old = MemoryContextSwitchTo(scan->cxt);

			memset(&spec, 0, sizeof(spec));
			spec.limit = -1;
			scan->reader = lt_reader_begin(rel, sscan->rs_snapshot, &spec);
			scan->started = true;
			MemoryContextSwitchTo(old);
		}
		scan->sample_block_ntuples = 0;
		if (scan->reader == NULL)
			return false;
		for (int i = 0; i < LT_ROWS_PER_BLOCK; i++)
		{
			if (scan->sample_slots[i] == NULL)
			{
				MemoryContext old = MemoryContextSwitchTo(scan->cxt);

				scan->sample_slots[i] = MakeSingleTupleTableSlot(RelationGetDescr(rel), lt_slot_ops_of());
				MemoryContextSwitchTo(old);
			}
			if (!lt_reader_next(scan->reader, scan->sample_slots[i]))
				break;
			ExecMaterializeSlot(scan->sample_slots[i]);
			scan->sample_block_ntuples++;
		}
		scan->sample_row = 0;
		if (scan->sample_block_ntuples == 0)
		{
			lt_scan_end_reader(scan);
			scan->started = true;
			return false;
		}
		return true;
	}
	/* the sample's next row, which stands for its block */
	scan->sample_row++;
	scan->sample_taken = false;
	return scan->sample_row < scan->sample.nrows;
}

static bool
lt_scan_analyze_next_tuple(TableScanDesc sscan, double *liverows, double *deadrows,
						   TupleTableSlot *slot)
{
	LtScanDesc	scan = (LtScanDesc) sscan;

	if (scan->sample_all)
	{
		if (scan->sample_row >= scan->sample_block_ntuples)
			return false;
		ExecCopySlot(slot, scan->sample_slots[scan->sample_row++]);
		*liverows += 1;
		return true;
	}
	if (scan->sample_taken || scan->sample_row < 0 || scan->sample_row >= scan->sample.nrows)
		return false;
	{
		/* the row's values: the sample's columns are the layout's in order */
		int			n = 0;

		ExecClearTuple(slot);
		memset(slot->tts_isnull, true, sizeof(bool) * slot->tts_tupleDescriptor->natts);
		for (int i = 0; i < scan->layout->natts; i++)
		{
			if (scan->layout->cols[i].kind == LT_NONE)
				continue;
			slot->tts_values[i] = lt_batch_datum(&scan->sample, n, scan->sample_row,
												 &slot->tts_isnull[i]);
			n++;
		}
		/* ANALYZE orders its sample by TID: the virtual block's */
		ItemPointerSet(&slot->tts_tid, scan->sample_blocks[scan->sample_row], FirstOffsetNumber);
		ExecStoreVirtualTuple(slot);
	}
	/* one row of the block, which stands for its rows */
	*liverows += scan->sample_block_rows[scan->sample_row];
	scan->sample_taken = true;
	return true;
}

/* ------------------------------------------------------------------------
 * Index builds
 * ------------------------------------------------------------------------
 */

static void
add_attnums(Node *node, Bitmapset **attrs)
{
	if (node)
		pull_varattnos(node, 1, attrs);
}

/* A read of the attributes an index's keys, expressions and predicate read */
static void
index_read_spec(IndexInfo *index_info, LtReadSpec *spec)
{
	Bitmapset  *attrs = NULL;
	int			k = -1;

	for (int i = 0; i < index_info->ii_NumIndexAttrs; i++)
		attrs = bms_add_member(attrs, index_info->ii_IndexAttrNumbers[i] -
							   FirstLowInvalidHeapAttributeNumber);
	add_attnums((Node *) index_info->ii_Expressions, &attrs);
	add_attnums((Node *) index_info->ii_Predicate, &attrs);
	memset(spec, 0, sizeof(*spec));
	spec->limit = -1;
	while ((k = bms_next_member(attrs, k)) >= 0)
	{
		AttrNumber	attno = k + FirstLowInvalidHeapAttributeNumber;

		if (attno <= 0)
		{
			/* a whole row or a system column: every column */
			list_free(spec->attnums);
			spec->attnums = NIL;
			break;
		}
		spec->attnums = lappend_int(spec->attnums, attno);
	}
}

static double
lt_index_build_range_scan(Relation table_rel, Relation index_rel, IndexInfo *index_info,
						  bool allow_sync, bool anyvisible, bool progress,
						  BlockNumber start_blockno, BlockNumber numblocks,
						  IndexBuildCallback callback, void *callback_state,
						  TableScanDesc scan)
{
	Snapshot	snapshot;
	bool		registered = false;
	EState	   *estate;
	ExprContext *econtext;
	TupleTableSlot *slot;
	ExprState  *predicate;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	double		reltuples = 0;
	LtReader   *reader;
	LtReadSpec	spec;

	/*
	 * BRIN summarizes ranges of physical blocks; a lance table's TIDs are
	 * row ids, from block 2^31 up, which no range of blocks summarizes.
	 */
	if (index_rel->rd_rel->relam == BRIN_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("BRIN indexes are not supported on lance tables"),
				 errhint("Lance's own min/max statistics prune a filter's reads: CREATE INDEX ... USING lance_index (...) WITH (type = 'zonemap').")));
	if (start_blockno != 0 || numblocks != InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lance tables do not support index builds over block ranges")));
	if (scan)
		snapshot = scan->rs_snapshot;
	else
	{
		snapshot = RegisterSnapshot(GetTransactionSnapshot());
		registered = true;
	}

	index_read_spec(index_info, &spec);

	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(table_rel, NULL);
	econtext->ecxt_scantuple = slot;
	predicate = ExecPrepareQual(index_info->ii_Predicate, estate);

	/*
	 * A parallel build's participants each read the share of the table the
	 * scan they are given takes (its fragments, one at a time); a build of
	 * one reads the columns the index needs.
	 */
	reader = scan ? NULL : lt_reader_begin(table_rel, snapshot, &spec);
	while (reader ? lt_reader_next(reader, slot) :
		   table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();
		MemoryContextReset(econtext->ecxt_per_tuple_memory);
		reltuples += 1;
		if (predicate != NULL && !ExecQual(predicate, econtext))
			continue;
		FormIndexDatum(index_info, slot, estate, values, isnull);
		callback(index_rel, &slot->tts_tid, values, isnull, true, callback_state);
	}
	if (reader)
		lt_reader_end(reader);

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	if (registered)
		UnregisterSnapshot(snapshot);
	index_info->ii_ExpressionsState = NIL;
	index_info->ii_PredicateState = NULL;
	return reltuples;
}

static int
cmp_int64(const void *a, const void *b)
{
	int64		x = *(const int64 *) a;
	int64		y = *(const int64 *) b;

	return x < y ? -1 : x > y ? 1 : 0;
}

/*
 * CREATE INDEX CONCURRENTLY's and REINDEX CONCURRENTLY's validation: the
 * rows the reference snapshot sees that the index has no entry for --
 * committed by transactions that began before the index took entries --
 * inserted into it.  The index's TIDs come sorted; the table's rows, in no
 * TID order, are looked up among them.
 */
static void
lt_index_validate_scan(Relation table_rel, Relation index_rel, IndexInfo *index_info,
					   Snapshot snapshot, ValidateIndexState *state)
{
	int64	   *tids = NULL;
	int64		ntids = 0;
	int64		cap = 0;
	Datum		ts_val;
	bool		ts_isnull;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	LtReadSpec	spec;
	LtReader   *reader;
	EState	   *estate;
	ExprContext *econtext;
	ExprState  *predicate;
	TupleTableSlot *slot;

	while (tuplesort_getdatum(state->tuplesort, true, false, &ts_val, &ts_isnull, NULL))
	{
		if (ts_isnull)
			continue;
		if (ntids == cap)
		{
			cap = Max(1024, cap * 2);
			tids = tids ? repalloc_huge(tids, sizeof(int64) * cap) :
				palloc_extended(sizeof(int64) * cap, MCXT_ALLOC_HUGE);
		}
		tids[ntids++] = DatumGetInt64(ts_val);
		state->itups += 1;
	}

	index_read_spec(index_info, &spec);
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(table_rel, NULL);
	econtext->ecxt_scantuple = slot;
	predicate = ExecPrepareQual(index_info->ii_Predicate, estate);
	reader = lt_reader_begin(table_rel, snapshot, &spec);
	while (lt_reader_next(reader, slot))
	{
		int64		tid = itemptr_encode(&slot->tts_tid);

		CHECK_FOR_INTERRUPTS();
		state->htups += 1;
		if (ntids > 0 && bsearch(&tid, tids, ntids, sizeof(int64), cmp_int64) != NULL)
			continue;
		MemoryContextReset(econtext->ecxt_per_tuple_memory);
		if (predicate != NULL && !ExecQual(predicate, econtext))
			continue;
		FormIndexDatum(index_info, slot, estate, values, isnull);
		index_insert(index_rel, values, isnull, &slot->tts_tid, table_rel,
					 index_info->ii_Unique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
					 false, index_info);
		state->tups_inserted += 1;
	}
	lt_reader_end(reader);
	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
	index_info->ii_ExpressionsState = NIL;
	index_info->ii_PredicateState = NULL;
	if (tids)
		pfree(tids);
}

/* ------------------------------------------------------------------------
 * Sizes
 * ------------------------------------------------------------------------
 */

static bool
table_info(Relation rel, LeDatasetInfo *info)
{
	LtTable    *t = lt_table_copy_by_relid(RelationGetRelid(rel), true);
	uint64		version;
	LeDataset  *ds;
	LeError		err = {0};
	int			rc;

	memset(info, 0, sizeof(*info));
	if (t == NULL)
		return false;
	version = lt_snapshot_version(rel, t, NULL);
	ds = lt_open_dataset(t, version);
	rc = lt_api()->dataset_info(ds, info, &err);
	lt_api()->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "reading a lance table's size");
	return true;
}

static uint64
lt_relation_size(Relation rel, ForkNumber forkNumber)
{
	LeDatasetInfo info;
	uint64		blocks;

	if (forkNumber != MAIN_FORKNUM)
		return 0;
	if (!table_info(rel, &info))
		return 0;
	blocks = ((uint64) lt_xact_rows(RelationGetRelid(rel), (double) info.num_rows) +
			  LT_ROWS_PER_BLOCK - 1) / LT_ROWS_PER_BLOCK;
	return blocks * BLCKSZ;
}

static bool
lt_relation_needs_toast_table(Relation rel)
{
	return false;
}

static Oid
lt_relation_toast_am(Relation rel)
{
	return InvalidOid;
}

static void
lt_relation_fetch_toast_slice(Relation toastrel, Oid valueid, int32 attrsize, int32 sliceoffset,
							  int32 slicelength, varlena *result)
{
	elog(ERROR, "lance tables have no TOAST table");
}

/*
 * The planner's estimate: the rows and the data's size the latest
 * committed version's manifest records, without reading a data file.
 */
static void
lt_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages, double *tuples,
						  double *allvisfrac)
{
	LeDatasetInfo info;

	*allvisfrac = 0;
	if (!table_info(rel, &info))
	{
		*pages = 0;
		*tuples = 0;
		return;
	}
	*tuples = lt_xact_rows(RelationGetRelid(rel), (double) info.num_rows);
	if (info.num_rows > 0)
	{
		/* the data files' bytes, the rows the transaction adds at their width */
		double		bytes = (double) info.data_bytes * (*tuples / (double) info.num_rows);

		*pages = (BlockNumber) Min((double) MaxBlockNumber, Max(bytes / BLCKSZ, 1));
	}
	else
	{
		/* the transaction's rows alone: their width, as heap's planner reckons it */
		double		bytes = *tuples * (get_rel_data_width(rel, attr_widths) + 24);

		*pages = (BlockNumber) Min((double) MaxBlockNumber,
								   *tuples > 0 ? Max(ceil(bytes / BLCKSZ), 1) : 0);
	}
}

/* ------------------------------------------------------------------------
 * Bitmap scans: TIDs a batch at a time
 * ------------------------------------------------------------------------
 */

static bool
bitmap_fill(LtScanDesc scan, uint64 *lossy_pages, uint64 *exact_pages)
{
	TBMIterateResult tbmres;
	OffsetNumber offsets[TBM_MAX_TUPLES_PER_PAGE];

	scan->bm_n = 0;
	scan->bm_pos = 0;
	scan->bm_recheck = false;
	while (scan->bm_n + LT_ROWS_PER_BLOCK <= BITMAP_FETCH_ROWS)
	{
		int			noffsets;

		if (!tbm_iterate(&scan->base.st.rs_tbmiterator, &tbmres))
		{
			scan->bm_done = true;
			break;
		}
		if (tbmres.lossy)
		{
			for (int i = 0; i < LT_ROWS_PER_BLOCK; i++)
				scan->bm_rowids[scan->bm_n++] = ((uint64) tbmres.blockno << 8) | (uint64) i;
			(*lossy_pages)++;
			scan->bm_recheck = true;
		}
		else
		{
			noffsets = tbm_extract_page_tuple(&tbmres, offsets, TBM_MAX_TUPLES_PER_PAGE);
			for (int i = 0; i < noffsets; i++)
				if (offsets[i] >= 1 && offsets[i] <= LT_ROWS_PER_BLOCK)
					scan->bm_rowids[scan->bm_n++] = ((uint64) tbmres.blockno << 8) |
						(uint64) (offsets[i] - 1);
			(*exact_pages)++;
		}
		if (tbmres.recheck)
			scan->bm_recheck = true;
	}
	if (scan->bm_n == 0)
		return false;
	lt_fetch_rows(scan->base.rs_rd, scan->base.rs_snapshot, scan->bm_rowids, scan->bm_n,
				  scan->bm_slots, scan->bm_found);
	return true;
}

static bool
lt_scan_bitmap_next_tuple(TableScanDesc sscan, TupleTableSlot *slot, bool *recheck,
						  uint64 *lossy_pages, uint64 *exact_pages)
{
	LtScanDesc	scan = (LtScanDesc) sscan;

	if (scan->bm_slots == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(scan->cxt);

		scan->bm_slots = palloc0(sizeof(TupleTableSlot *) * BITMAP_FETCH_ROWS);
		for (int i = 0; i < BITMAP_FETCH_ROWS; i++)
			scan->bm_slots[i] = MakeSingleTupleTableSlot(RelationGetDescr(sscan->rs_rd), lt_slot_ops_of());
		scan->bm_found = palloc0(sizeof(bool) * BITMAP_FETCH_ROWS);
		scan->bm_rowids = palloc0(sizeof(uint64) * BITMAP_FETCH_ROWS);
		MemoryContextSwitchTo(old);
	}
	for (;;)
	{
		while (scan->bm_pos < scan->bm_n)
		{
			int			i = scan->bm_pos++;

			if (!scan->bm_found[i])
				continue;
			ExecCopySlot(slot, scan->bm_slots[i]);
			slot->tts_tid = scan->bm_slots[i]->tts_tid;
			slot->tts_tableOid = RelationGetRelid(sscan->rs_rd);
			*recheck = scan->bm_recheck;
			pgstat_count_heap_fetch(sscan->rs_rd);
			return true;
		}
		if (scan->bm_done || !bitmap_fill(scan, lossy_pages, exact_pages))
			return false;
	}
}

/* ------------------------------------------------------------------------
 * TABLESAMPLE: virtual blocks in the order of a scan
 * ------------------------------------------------------------------------
 */

static bool
lt_scan_sample_next_block(TableScanDesc sscan, SampleScanState *scanstate)
{
	LtScanDesc	scan = (LtScanDesc) sscan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	LeDatasetInfo info;
	BlockNumber nblocks;
	BlockNumber want;

	if (!table_info(sscan->rs_rd, &info))
		return false;
	nblocks = (BlockNumber) Min((uint64) MaxBlockNumber,
								(info.num_rows + LT_ROWS_PER_BLOCK - 1) / LT_ROWS_PER_BLOCK);
	if (tsm->NextSampleBlock)
	{
		want = tsm->NextSampleBlock(scanstate, nblocks);
		if (!BlockNumberIsValid(want))
			return false;
	}
	else
	{
		want = scan->started ? scan->sample_block + 1 : 0;
		if (want >= nblocks)
			return false;
	}
	if (!scan->started || want < scan->sample_block)
	{
		/* begin again: blocks are read in a scan's order */
		LtReadSpec	spec;
		MemoryContext old = MemoryContextSwitchTo(scan->cxt);

		lt_scan_end_reader(scan);
		memset(&spec, 0, sizeof(spec));
		spec.limit = -1;
		scan->reader = lt_reader_begin(sscan->rs_rd, sscan->rs_snapshot, &spec);
		scan->started = true;
		scan->sample_block = 0;
		scan->sample_block_ntuples = -1;
		MemoryContextSwitchTo(old);
	}
	/* skip to the block, a block a run of LT_ROWS_PER_BLOCK rows */
	while (scan->sample_block_ntuples < 0 || scan->sample_block < want)
	{
		if (scan->sample_block_ntuples >= 0)
			scan->sample_block++;
		scan->sample_block_ntuples = 0;
		for (int i = 0; i < LT_ROWS_PER_BLOCK; i++)
		{
			if (scan->sample_slots[i] == NULL)
			{
				MemoryContext old = MemoryContextSwitchTo(scan->cxt);

				scan->sample_slots[i] = MakeSingleTupleTableSlot(RelationGetDescr(sscan->rs_rd), lt_slot_ops_of());
				MemoryContextSwitchTo(old);
			}
			if (!lt_reader_next(scan->reader, scan->sample_slots[i]))
				break;
			ExecMaterializeSlot(scan->sample_slots[i]);
			scan->sample_block_ntuples++;
		}
		if (scan->sample_block_ntuples == 0)
			return false;
	}
	return true;
}

static bool
lt_scan_sample_next_tuple(TableScanDesc sscan, SampleScanState *scanstate, TupleTableSlot *slot)
{
	LtScanDesc	scan = (LtScanDesc) sscan;
	TsmRoutine *tsm = scanstate->tsmroutine;

	for (;;)
	{
		OffsetNumber off = tsm->NextSampleTuple(scanstate, scan->sample_block,
												(OffsetNumber) scan->sample_block_ntuples);

		if (!OffsetNumberIsValid(off))
			return false;
		if (off > scan->sample_block_ntuples)
			continue;
		ExecCopySlot(slot, scan->sample_slots[off - 1]);
		slot->tts_tid = scan->sample_slots[off - 1]->tts_tid;
		slot->tts_tableOid = RelationGetRelid(sscan->rs_rd);
		return true;
	}
}

/*
 * A lance table's slots: virtual slots, every value stored at once, with
 * the system columns a heap tuple's header would give -- xmin, which ON
 * CONFLICT DO UPDATE and foreign keys' triggers ask of a row, is the
 * transaction's own for a row it inserted and frozen for a committed one.
 */
static TupleTableSlotOps lt_slot_ops;
static bool lt_slot_ops_made = false;

static bool
slot_row_is_own(TupleTableSlot *slot)
{
	if (!ItemPointerIsValid(&slot->tts_tid) || !OidIsValid(slot->tts_tableOid))
		return false;
	return lt_xact_row_is_own(slot->tts_tableOid, lt_tid_to_rowid(&slot->tts_tid));
}

static void
lt_slot_getsomeattrs(TupleTableSlot *slot, int natts)
{
	if (TTS_EMPTY(slot))
		elog(ERROR, "lance_tam: attributes of an empty slot");
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
}

static Datum
lt_slot_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	Assert(!TTS_EMPTY(slot));
	*isnull = false;
	switch (attnum)
	{
		case MinTransactionIdAttributeNumber:
			return TransactionIdGetDatum(slot_row_is_own(slot) ?
										 GetCurrentTransactionIdIfAny() : FrozenTransactionId);
		case MaxTransactionIdAttributeNumber:
			return TransactionIdGetDatum(InvalidTransactionId);
		case MinCommandIdAttributeNumber:
		case MaxCommandIdAttributeNumber:
			return CommandIdGetDatum(FirstCommandId);
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("lance tables have no system column %d", attnum)));
	}
	pg_unreachable();
}

static bool
lt_slot_is_current_xact_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));
	return slot_row_is_own(slot);
}

/*
 * A copy of a row carries its TID, as a copy of a heap tuple does: ANALYZE
 * orders its sample by the tuples' t_self, a trigger reads the row's.
 */
static void
lt_slot_copyslot(TupleTableSlot *dst, TupleTableSlot *src)
{
	ItemPointerData tid = src->tts_tid;
	Oid			tableoid = src->tts_tableOid;

	TTSOpsVirtual.copyslot(dst, src);
	dst->tts_tid = tid;
	dst->tts_tableOid = tableoid;
}

static HeapTuple
lt_slot_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple	tuple = TTSOpsVirtual.copy_heap_tuple(slot);

	tuple->t_self = slot->tts_tid;
	tuple->t_tableOid = slot->tts_tableOid;
	return tuple;
}

static const TupleTableSlotOps *
lt_slot_callbacks(Relation rel)
{
	if (!lt_slot_ops_made)
	{
		lt_slot_ops = TTSOpsVirtual;
		lt_slot_ops.getsomeattrs = lt_slot_getsomeattrs;
		lt_slot_ops.getsysattr = lt_slot_getsysattr;
		lt_slot_ops.is_current_xact_tuple = lt_slot_is_current_xact_tuple;
		lt_slot_ops.copyslot = lt_slot_copyslot;
		lt_slot_ops.copy_heap_tuple = lt_slot_copy_heap_tuple;
		lt_slot_ops_made = true;
	}
	return &lt_slot_ops;
}

const TupleTableSlotOps *
lt_slot_ops_of(void)
{
	return lt_slot_callbacks(NULL);
}

static const TableAmRoutine lt_routine = {
	.type = T_TableAmRoutine,

	.slot_callbacks = lt_slot_callbacks,

	.scan_begin = lt_scan_begin,
	.scan_end = lt_scan_end,
	.scan_rescan = lt_scan_rescan,
	.scan_getnextslot = lt_scan_getnextslot,

	.scan_set_tidrange = lt_scan_set_tidrange,
	.scan_getnextslot_tidrange = lt_scan_getnextslot_tidrange,

	.parallelscan_estimate = lt_parallelscan_estimate,
	.parallelscan_initialize = lt_parallelscan_initialize,
	.parallelscan_reinitialize = lt_parallelscan_reinitialize,

	.index_fetch_begin = lt_index_fetch_begin,
	.index_fetch_reset = lt_index_fetch_reset,
	.index_fetch_end = lt_index_fetch_end,
	.index_fetch_tuple = lt_index_fetch_tuple,

	.tuple_fetch_row_version = lt_tuple_fetch_row_version,
	.tuple_tid_valid = lt_tuple_tid_valid,
	.tuple_get_latest_tid = lt_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = lt_tuple_satisfies_snapshot,
	.index_delete_tuples = lt_index_delete_tuples,

	.tuple_insert = lt_tuple_insert,
	.tuple_insert_speculative = lt_tuple_insert_speculative,
	.tuple_complete_speculative = lt_tuple_complete_speculative,
	.multi_insert = lt_multi_insert,
	.tuple_delete = lt_tuple_delete,
	.tuple_update = lt_tuple_update,
	.tuple_lock = lt_tuple_lock,
	.finish_bulk_insert = lt_finish_bulk_insert,

	.relation_set_new_filelocator = lt_relation_set_new_filelocator,
	.relation_nontransactional_truncate = lt_relation_nontransactional_truncate,
	.relation_copy_data = lt_relation_copy_data,
	.relation_copy_for_cluster = lt_relation_copy_for_cluster,
	.relation_vacuum = lt_relation_vacuum,
	.scan_analyze_next_block = lt_scan_analyze_next_block,
	.scan_analyze_next_tuple = lt_scan_analyze_next_tuple,
	.index_build_range_scan = lt_index_build_range_scan,
	.index_validate_scan = lt_index_validate_scan,

	.relation_size = lt_relation_size,
	.relation_needs_toast_table = lt_relation_needs_toast_table,
	.relation_toast_am = lt_relation_toast_am,
	.relation_fetch_toast_slice = lt_relation_fetch_toast_slice,

	.relation_estimate_size = lt_relation_estimate_size,

	.scan_bitmap_next_tuple = lt_scan_bitmap_next_tuple,

	.scan_sample_next_block = lt_scan_sample_next_block,
	.scan_sample_next_tuple = lt_scan_sample_next_tuple
};

PG_FUNCTION_INFO_V1(lance_tableam_handler);

Datum
lance_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&lt_routine);
}
