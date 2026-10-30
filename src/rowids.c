/*-------------------------------------------------------------------------
 *
 * rowids.c
 *	  The row ids no snapshot will see again (docs/design.md §2.6), whose
 *	  entries in PostgreSQL's indexes are dead as the entries of heap's dead
 *	  tuples are: the rows of a transaction that rolled back, the rows a
 *	  commit deleted.
 *
 *	  A row id is dead when the latest version the catalog names has no row
 *	  of it, every snapshot reads that version -- the transaction that named
 *	  it is past every snapshot's horizon -- and no open transaction has
 *	  taken the id.  A row id is never given twice, so once the transaction
 *	  that took it has ended, its row is in a version or in none, for good.
 *	  So each backend shows in shared memory the ids its open transaction
 *	  takes: from the first it took to the end of the last chunk it took
 *	  them from, the chunks of other backends between counted as well.
 *
 *	  A fetch through an index that finds no row of a dead id says so
 *	  (all_dead): the index marks the entry dead for every scan after, and
 *	  removes it when its page fills.  VACUUM removes the entries of every
 *	  dead id from the table's indexes (lt_vacuum_indexes()), against the
 *	  row ids of the latest version; a transaction that ends as it does so
 *	  may have rows that version has not, so the ids of those transactions
 *	  are kept in shared memory until the last VACUUM at it has finished.
 *
 *	  Without the shared memory of shared_preload_libraries no row id is
 *	  ever dead.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "commands/vacuum.h"
#include "common/int.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "lance_tam.h"

/* The ids a backend's open transaction took or may take: [start, end) */
typedef struct OpenRowids
{
	bool		open;
	uint64		start;
	uint64		end;
} OpenRowids;

/* The ids of the transactions that ended while a VACUUM removed entries */
#define MAX_ENDED	1024

/* How long a VACUUM waits for every snapshot to read the version it read */
#define SETTLE_WAIT_MS	1000
#define SETTLE_POLL_MS	10

typedef struct RowidsShared
{
	LWLock	   *lock;
	int			vacuums;		/* VACUUMs removing entries now */
	int			nended;
	bool		ended_lost;		/* more ended than there is room for */
	uint64		ended_start[MAX_ENDED];
	uint64		ended_end[MAX_ENDED];
	OpenRowids	backends[FLEXIBLE_ARRAY_MEMBER];
} RowidsShared;

static RowidsShared *shared = NULL;
static shmem_request_hook_type prev_request_hook = NULL;
static shmem_startup_hook_type prev_startup_hook = NULL;

/* This backend's open transaction has shown the ids it takes */
static bool shown = false;
static bool exit_registered = false;

static Size
shared_size(void)
{
	return add_size(offsetof(RowidsShared, backends),
					mul_size(MaxBackends, sizeof(OpenRowids)));
}

static void
request_hook(void)
{
	if (prev_request_hook)
		prev_request_hook();
	RequestAddinShmemSpace(shared_size());
	RequestNamedLWLockTranche("lance_tam rowids", 1);
}

static void
startup_hook(void)
{
	bool		found;

	if (prev_startup_hook)
		prev_startup_hook();
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shared = ShmemInitStruct("lance_tam rowids", shared_size(), &found);
	if (!found)
	{
		memset(shared, 0, shared_size());
		shared->lock = &(GetNamedLWLockTranche("lance_tam rowids"))->lock;
	}
	LWLockRelease(AddinShmemInitLock);
}

void
lt_rowids_init(void)
{
	prev_request_hook = shmem_request_hook;
	shmem_request_hook = request_hook;
	prev_startup_hook = shmem_startup_hook;
	shmem_startup_hook = startup_hook;
}

/* A backend that exits leaves no ids shown, whatever it was doing */
static void
rowids_exit(int code, Datum arg)
{
	lt_rowids_xact_end();
}

bool
lt_rowids_shown(void)
{
	return shown || shared == NULL;
}

/*
 * The open transaction takes ids from "start" up to "end", its backend's
 * chunk's: shown before the first of them is given, so that no index has
 * an entry of one before another backend can tell it is open.
 */
void
lt_rowids_show(uint64 start, uint64 end)
{
	OpenRowids *b;

	if (shared == NULL)
		return;
	if (!exit_registered)
	{
		before_shmem_exit(rowids_exit, (Datum) 0);
		exit_registered = true;
	}
	Assert(MyProcNumber >= 0 && MyProcNumber < MaxBackends);
	b = &shared->backends[MyProcNumber];
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);
	b->start = shown ? Min(b->start, start) : start;
	b->end = shown ? Max(b->end, end) : end;
	b->open = true;
	LWLockRelease(shared->lock);
	shown = true;
}

/*
 * The transaction has ended: past its commit's being visible, at the
 * callback of its end (lt_rowid_dead() relies on this order).
 */
void
lt_rowids_xact_end(void)
{
	OpenRowids *b;

	if (!shown)
		return;
	shown = false;
	b = &shared->backends[MyProcNumber];
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);
	if (shared->vacuums > 0)
	{
		if (shared->nended < MAX_ENDED)
		{
			shared->ended_start[shared->nended] = b->start;
			shared->ended_end[shared->nended] = b->end;
			shared->nended++;
		}
		else
			shared->ended_lost = true;
	}
	b->open = false;
	LWLockRelease(shared->lock);
}

/* Whether an open transaction may have taken the id; the lock held */
static bool
rowid_open(uint64 rowid)
{
	for (int i = 0; i < MaxBackends; i++)
	{
		OpenRowids *b = &shared->backends[i];

		if (b->open && rowid >= b->start && rowid < b->end)
			return true;
	}
	return false;
}

/*
 * Whether no snapshot will see the row id again, which a fetch by the
 * snapshot found no row of.  A transaction that has deleted, updated in
 * place or truncated rows of the table finds no row of a row that others
 * still see, and asks nothing.  A table whose dataset other writers change,
 * or that follows it, has its versions from outside.  A parallel operation
 * cannot take the latest snapshot: a fetch outside one tells.
 */
bool
lt_rowid_dead(Relation rel, uint64 rowid, Snapshot snapshot)
{
	/* The last snapshot that read a version before the latest, as it will for good */
	static Oid	behind_relid = InvalidOid;
	static TransactionId behind_xmin;
	static TransactionId behind_xmax;
	static CommandId behind_cid;

	LtTable    *t = lt_table(rel);
	Oid			relid = RelationGetRelid(rel);
	bool		mvcc = IsMVCCSnapshot(snapshot);
	uint64		latest;
	uint64		version;
	TransactionId xid;
	bool		open;
	bool		settled;

	if (shared == NULL || IsInParallelMode() || !t->writable || t->follow ||
		!lt_xact_only_appended(relid))
		return false;
	if (mvcc && relid == behind_relid && snapshot->xmin == behind_xmin &&
		snapshot->xmax == behind_xmax && snapshot->curcid == behind_cid)
		return false;

	/*
	 * In this order: a transaction seen ended here has its commit in the
	 * version read next, so that its rows are not taken for dead by a fetch
	 * of the version before.
	 */
	LWLockAcquire(shared->lock, LW_SHARED);
	open = rowid_open(rowid);
	LWLockRelease(shared->lock);
	if (open)
		return false;
	settled = lt_catalog_latest(relid, &latest, &xid);

	/* a dirty snapshot's version is the latest, as it was for the fetch */
	version = snapshot->snapshot_type == SNAPSHOT_DIRTY ? latest : lt_snapshot_version(rel, t, snapshot);
	if (mvcc && latest > version)
	{
		behind_relid = relid;
		behind_xmin = snapshot->xmin;
		behind_xmax = snapshot->xmax;
		behind_cid = snapshot->curcid;
	}
	return settled && latest == version;
}

/* ------------------------------------------------------------------------
 * VACUUM: the entries of dead row ids removed from the indexes
 * ------------------------------------------------------------------------
 */

/* The row ids of a version's live rows, as runs: [start, end) */
typedef struct RowidRun
{
	uint64		start;
	uint64		end;
} RowidRun;

typedef struct LiveRowids
{
	RowidRun   *runs;
	int64		nruns;
	int64		cap;
	int64		max_runs;		/* what maintenance_work_mem holds */
	bool		too_many;
	uint64		rows;
	int64		removed;
} LiveRowids;

static void
live_add(uint64 rowid, void *arg)
{
	LiveRowids *l = arg;

	l->rows++;
	if (l->too_many)
		return;
	if (l->nruns > 0 && l->runs[l->nruns - 1].end == rowid)
	{
		l->runs[l->nruns - 1].end = rowid + 1;
		return;
	}
	if (l->nruns >= l->max_runs)
	{
		l->too_many = true;
		return;
	}
	if (l->nruns >= l->cap)
	{
		l->cap = Min(l->cap * 2, l->max_runs);
		l->runs = repalloc_huge(l->runs, sizeof(RowidRun) * l->cap);
	}
	l->runs[l->nruns].start = rowid;
	l->runs[l->nruns].end = rowid + 1;
	l->nruns++;
}

static int
run_cmp(const void *a, const void *b)
{
	return pg_cmp_u64(((const RowidRun *) a)->start, ((const RowidRun *) b)->start);
}

/* In the order of their ids, which fragments need not give them in */
static void
live_sort(LiveRowids *l)
{
	int64		n = 0;

	qsort(l->runs, l->nruns, sizeof(RowidRun), run_cmp);
	for (int64 i = 0; i < l->nruns; i++)
	{
		if (n > 0 && l->runs[i].start <= l->runs[n - 1].end)
			l->runs[n - 1].end = Max(l->runs[n - 1].end, l->runs[i].end);
		else
			l->runs[n++] = l->runs[i];
	}
	l->nruns = n;
}

static bool
live_has(LiveRowids *l, uint64 rowid)
{
	int64		lo = 0;
	int64		hi = l->nruns - 1;

	while (lo <= hi)
	{
		int64		mid = lo + (hi - lo) / 2;

		if (rowid < l->runs[mid].start)
			hi = mid - 1;
		else if (rowid >= l->runs[mid].end)
			lo = mid + 1;
		else
			return true;
	}
	return false;
}

/* Whether a transaction that ended since the VACUUM began took the id; the lock held */
static bool
rowid_ended(uint64 rowid)
{
	for (int i = 0; i < shared->nended; i++)
		if (rowid >= shared->ended_start[i] && rowid < shared->ended_end[i])
			return true;
	return false;
}

/* The index's callback: whether the entry's row id is dead */
static bool
entry_dead(ItemPointer tid, void *arg)
{
	LiveRowids *l = arg;
	uint64		rowid;
	bool		dead;

	if (!lt_tid_valid(tid))
		return false;
	rowid = lt_tid_to_rowid(tid);
	if (live_has(l, rowid))
		return false;
	LWLockAcquire(shared->lock, LW_SHARED);
	dead = !shared->ended_lost && !rowid_open(rowid) && !rowid_ended(rowid);
	LWLockRelease(shared->lock);
	if (dead)
		l->removed++;
	return dead;
}

static bool vacuuming = false;

static void
vacuum_begin(void)
{
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);
	if (shared->vacuums++ == 0)
	{
		shared->nended = 0;
		shared->ended_lost = false;
	}
	LWLockRelease(shared->lock);
	vacuuming = true;
}

static void
vacuum_end(int code, Datum arg)
{
	if (!vacuuming)
		return;
	vacuuming = false;
	LWLockAcquire(shared->lock, LW_EXCLUSIVE);
	shared->vacuums--;
	LWLockRelease(shared->lock);
}

static void
remove_entries(Relation rel, Relation *indexes, int nindexes, BufferAccessStrategy bstrategy,
			   int elevel)
{
	MemoryContext cxt = AllocSetContextCreate(CurrentMemoryContext, "lance_tam live row ids",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContext old = MemoryContextSwitchTo(cxt);
	int			mem = AmAutoVacuumWorkerProcess() && autovacuum_work_mem != -1 ?
		autovacuum_work_mem : maintenance_work_mem;
	LiveRowids	live;
	uint64		version;
	TransactionId xid;
	bool		settled;

	/*
	 * After vacuum_begin(): a transaction that ended before it has its
	 * commit in the version read here, one that ends after it is noted.
	 * The version need not stay the latest: under steady commits a newer
	 * one always has transactions at it, where this one wants no more than
	 * those at it now to end.  A long one keeps the entries, as it keeps
	 * heap's dead tuples.
	 */
	settled = lt_catalog_latest(RelationGetRelid(rel), &version, &xid);
	for (int waited = 0; !settled && waited < SETTLE_WAIT_MS; waited += SETTLE_POLL_MS)
	{
		CHECK_FOR_INTERRUPTS();
		pg_usleep(SETTLE_POLL_MS * 1000L);
		settled = lt_catalog_settled(xid);
	}
	if (!settled)
	{
		ereport(elevel,
				(errmsg("lance table \"%s\": index entries left: a snapshot may read a version before the latest",
						RelationGetRelationName(rel))));
		MemoryContextSwitchTo(old);
		MemoryContextDelete(cxt);
		return;
	}
	memset(&live, 0, sizeof(live));
	live.cap = 1024;
	live.max_runs = Max((int64) mem * 1024 / (int64) sizeof(RowidRun), live.cap);
	live.runs = palloc(sizeof(RowidRun) * live.cap);
	lt_scan_rowids(rel, version, live_add, &live);
	if (live.too_many)
	{
		ereport(elevel,
				(errmsg("lance table \"%s\": index entries left: its row ids take more than %d kB",
						RelationGetRelationName(rel), mem)));
		MemoryContextSwitchTo(old);
		MemoryContextDelete(cxt);
		return;
	}
	live_sort(&live);
	if (lt_debug_vacuum_wait != 0)
	{
		DirectFunctionCall1(pg_advisory_lock_int8, Int64GetDatum((int64) lt_debug_vacuum_wait));
		DirectFunctionCall1(pg_advisory_unlock_int8, Int64GetDatum((int64) lt_debug_vacuum_wait));
	}
	for (int i = 0; i < nindexes; i++)
	{
		IndexVacuumInfo ivinfo;
		IndexBulkDeleteResult *stats;

		/* Lance keeps its own */
		if (lt_is_lance_index_am(indexes[i]->rd_rel->relam))
			continue;
		ivinfo.index = indexes[i];
		ivinfo.heaprel = rel;
		ivinfo.analyze_only = false;
		ivinfo.is_autovacuum = AmAutoVacuumWorkerProcess();
		ivinfo.report_progress = false;
		ivinfo.estimated_count = false;
		ivinfo.message_level = elevel;
		ivinfo.num_heap_tuples = (double) live.rows;
		ivinfo.strategy = bstrategy;
		stats = index_bulk_delete(&ivinfo, NULL, entry_dead, &live);
		stats = index_vacuum_cleanup(&ivinfo, stats);
		if (stats && !stats->estimated_count)
			vac_update_relstats(indexes[i], stats->num_pages, stats->num_index_tuples, 0, 0, false,
								InvalidTransactionId, InvalidMultiXactId, NULL, NULL, false);
		if (stats)
			pfree(stats);
	}
	ereport(elevel,
			(errmsg("lance table \"%s\": removed %lld index entries of rows no snapshot will see",
					RelationGetRelationName(rel), (long long) live.removed)));
	MemoryContextSwitchTo(old);
	MemoryContextDelete(cxt);
}

/*
 * The entries of the table's dead row ids out of its PostgreSQL indexes.
 * A VACUUM command always looks; autovacuum when the table has dead rows,
 * which statistics count of a rollback too, since it runs for inserts alone
 * as well.
 */
void
lt_vacuum_indexes(Relation rel, const VacuumParams *params, BufferAccessStrategy bstrategy,
				  int elevel)
{
	LtTable    *t = lt_table(rel);
	Relation   *indexes;
	int			nindexes;
	bool		any = false;

	if (shared == NULL || !t->writable || t->follow || !rel->rd_rel->relhasindex ||
		params->index_cleanup == VACOPTVALUE_DISABLED)
		return;
	if (AmAutoVacuumWorkerProcess() && params->index_cleanup == VACOPTVALUE_AUTO)
	{
		PgStat_StatTabEntry *stat = pgstat_fetch_stat_tabentry(RelationGetRelid(rel));

		if (stat == NULL || stat->dead_tuples <= 0)
			return;
	}
	vac_open_indexes(rel, RowExclusiveLock, &nindexes, &indexes);
	for (int i = 0; i < nindexes; i++)
		if (!lt_is_lance_index_am(indexes[i]->rd_rel->relam))
			any = true;
	if (any)
	{
		PG_ENSURE_ERROR_CLEANUP(vacuum_end, (Datum) 0);
		{
			vacuum_begin();
			remove_entries(rel, indexes, nindexes, bstrategy, elevel);
		}
		PG_END_ENSURE_ERROR_CLEANUP(vacuum_end, (Datum) 0);
		vacuum_end(0, (Datum) 0);
	}
	vac_close_indexes(nindexes, indexes, NoLock);
}
