/*-------------------------------------------------------------------------
 *
 * xact.c
 *	  A transaction's changes to the lance tables it writes, and what its
 *	  commit and abort do with them (docs/design.md §2.6, §2.7).
 *
 *	  Rows inserted are formed into batches here, a batch a command and
 *	  subtransaction, and handed to the engine, which holds them in memory
 *	  and then in fragments written but not committed.  Rows deleted are
 *	  gathered here and handed over the same way.  A row's TID is final when
 *	  it is inserted: its row id comes from lance.rowid_seq.
 *
 *	  Commit, at XACT_EVENT_PRE_COMMIT, a table at a time:
 *		1. the table's commit lock, held to the end of the transaction, so
 *		   that its writers commit one after another;
 *		2. the version the catalog's latest row names;
 *		3. Lance's versions after it: one a transaction of this server
 *		   committed to Lance and then aborted is undone by restoring the
 *		   catalog's version; one another writer of the dataset committed
 *		   stays, and the commit is made on top of it;
 *		4. the changes committed as new versions, the deleted rows resolved
 *		   on that version: a row another transaction deleted first is a
 *		   serialization failure;
 *		5. the catalog row updated to the last version committed.
 *	  An abort after step 4 restores the version the catalog keeps; an abort
 *	  before it removes the files the transaction wrote.  A dataset made by
 *	  a CREATE TABLE that aborts is removed; one a DROP TABLE that commits
 *	  dropped is removed, when the server made it.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/index.h"
#include "catalog/pg_index.h"
#include "commands/sequence.h"
#include "executor/tuptable.h"
#include "tcop/utility.h"
#include "utils/syscache.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

#include "lance_tam.h"

static HTAB *xact_tables = NULL;

/*
 * From XACT_EVENT_PRE_COMMIT, when the transaction's changes become Lance
 * versions, to the transaction's end.
 */
static bool xact_committing = false;

/* deleted rows not yet handed to the engine: a command and subtransaction */
#define DELETE_FLUSH_ROWS	65536

static void lt_xact_callback(XactEvent event, void *arg);
static void lt_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
								SubTransactionId parentSubid, void *arg);

void
lt_xact_init(void)
{
	RegisterXactCallback(lt_xact_callback, NULL);
	RegisterSubXactCallback(lt_subxact_callback, NULL);
}

static HTAB *
tables_hash(bool create)
{
	if (xact_tables == NULL && create)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(LtXactTable);
		ctl.hcxt = TopTransactionContext;
		xact_tables = hash_create("lance_tam transaction tables", 16, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	return xact_tables;
}

LtXactTable *
lt_xact_table_by_relid(Oid relid)
{
	HTAB	   *h = tables_hash(false);

	if (h == NULL)
		return NULL;
	return hash_search(h, &relid, HASH_FIND, NULL);
}

static LtXactTable *
xact_table_enter(Oid relid, const char *uri, const char *storage_json, bool managed)
{
	HTAB	   *h = tables_hash(true);
	bool		found;
	LtXactTable *xt = hash_search(h, &relid, HASH_ENTER, &found);

	if (!found)
	{
		MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);
		Oid			key = xt->relid;

		memset(xt, 0, sizeof(*xt));
		xt->relid = key;
		xt->uri = pstrdup(uri);
		xt->storage_json = pstrdup(storage_json);
		xt->managed = managed;
		xt->delete_cid = InvalidCommandId;
		xt->cmd_cid = InvalidCommandId;
		MemoryContextSwitchTo(old);
	}
	return xt;
}

LtXactTable *
lt_xact_table(Relation rel, bool create)
{
	LtXactTable *xt = lt_xact_table_by_relid(RelationGetRelid(rel));
	LtTable    *t;

	if (xt || !create)
		return xt;
	t = lt_table(rel);
	return xact_table_enter(RelationGetRelid(rel), t->uri, t->storage_json, t->managed);
}

static bool
has_unique_index(Relation rel)
{
	List	   *indexes;
	ListCell   *lc;
	bool		unique = false;

	if (!rel->rd_rel->relhasindex)
		return false;
	indexes = RelationGetIndexList(rel);
	foreach(lc, indexes)
	{
		Oid			indexoid = lfirst_oid(lc);
		HeapTuple	tup = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));

		if (HeapTupleIsValid(tup))
		{
			Form_pg_index idx = (Form_pg_index) GETSTRUCT(tup);

			if (idx->indisunique || idx->indisexclusion)
				unique = true;
			ReleaseSysCache(tup);
		}
	}
	list_free(indexes);
	return unique;
}

LeWrite *
lt_xact_write(Relation rel, LtXactTable *xt)
{
	LtTable    *t;
	uint64		version;
	LeDataset  *ds;
	LeWriteSpec spec;
	LeError		err = {0};
	const LeApi *api;

	if (xt->write)
		return xt->write;
	if (xact_committing)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot write lance table \"%s\" once its transaction has committed its changes",
						RelationGetRelationName(rel))));

	t = lt_table(rel);
	if (!t->writable)
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("lance table \"%s\" is read-only", RelationGetRelationName(rel)),
				 errdetail("Its dataset has no stable row ids, or the table was attached with read_only.")));
	PreventCommandIfReadOnly("writing a lance table");
	if (IsInParallelMode())
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TRANSACTION_STATE),
				 errmsg("cannot write a lance table during a parallel operation")));

	/*
	 * A unique index's checks see the rows of committed transactions and of
	 * this one, not those of another writer in progress, whose rows are in
	 * its own backend: with a unique index, writers write one at a time.
	 */
	if (has_unique_index(rel))
	{
		lt_lock_table_commit(RelationGetRelid(rel));
		xt->serialize_writers = true;
	}

	api = lt_api();
	version = lt_catalog_version(RelationGetRelid(rel), NULL);
	ds = lt_open_dataset(t, version);
	PG_TRY();
	{
		lt_rowids_for(ds);

		memset(&spec, 0, sizeof(spec));
		spec.size = sizeof(spec);
		spec.max_rows_per_file = t->max_rows_per_file;
		spec.max_rows_per_group = t->max_rows_per_group;
		spec.buffer_rows = t->buffer_rows > 0 ? t->buffer_rows : lt_buffer_rows;
		spec.buffer_bytes = (int64) 256 * 1024 * 1024;
		spec.storage_version = t->storage_version;
		if (api->write_begin(t->uri, t->storage_json, ds, NULL, &spec, &xt->write, &err) != LE_OK)
			lt_raise(&err, "beginning a write of a lance table");
	}
	PG_FINALLY();
	{
		api->dataset_close(ds);
	}
	PG_END_TRY();
	xt->base_version = version;
	return xt->write;
}

/* The batch formed so far, handed to the engine */
static void
flush_builder(LtXactTable *xt)
{
	struct ArrowArray array;
	struct ArrowSchema schema;
	LeError		err = {0};
	int			rc;

	if (xt->builder == NULL || lt_builder_rows(xt->builder) == 0)
		return;
	memset(&array, 0, sizeof(array));
	memset(&schema, 0, sizeof(schema));
	lt_builder_export(xt->builder, &array, &schema);
	rc = lt_api()->write_append(xt->write, &array, &schema, lt_builder_rowids(xt->builder),
								(int64) xt->builder_cid, xt->builder_subxid, &err);
	if (array.release)
		array.release(&array);
	if (schema.release)
		schema.release(&schema);
	lt_builder_reset(xt->builder);
	if (rc != LE_OK)
		lt_raise(&err, "writing rows to a lance table");
}

static void
flush_deletes(LtXactTable *xt)
{
	LeError		err = {0};

	if (xt->ndeletes == 0)
		return;
	if (lt_api()->write_delete_read(xt->write, xt->deletes, xt->ndeletes, xt->delete_version,
									(int64) xt->delete_cid, xt->delete_subxid, &err) != LE_OK)
		lt_raise(&err, "deleting rows of a lance table");
	xt->ndeletes = 0;
}

void
lt_xact_flush(LtXactTable *xt)
{
	if (xt == NULL || xt->write == NULL)
		return;
	flush_builder(xt);
	flush_deletes(xt);
}

void
lt_xact_flush_rel(Relation rel)
{
	lt_xact_flush(lt_xact_table_by_relid(RelationGetRelid(rel)));
}

/*
 * A row into the batch being formed: a new row, under a new id, or with
 * "version" set, a new version of row "rowid", an in-place UPDATE's, which
 * keeps the row's id and so its TID (the engine's LE_ROWID_NEW_VERSION).
 */
static void
xact_insert(Relation rel, TupleTableSlot *slot, CommandId cid, uint64 rowid, bool version)
{
	LtXactTable *xt = lt_xact_table(rel, true);
	SubTransactionId subxid = GetCurrentSubTransactionId();

	lt_xact_write(rel, xt);
	if (xt->builder && lt_builder_rows(xt->builder) > 0 &&
		(xt->builder_cid != cid || xt->builder_subxid != subxid))
		flush_builder(xt);
	if (xt->builder == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

		xt->builder = lt_builder_create(lt_layout_build(rel, TopTransactionContext));
		MemoryContextSwitchTo(old);
	}
	xt->builder_cid = cid;
	xt->builder_subxid = subxid;

	if (!version)
		rowid = lt_next_rowid();
	lt_rowid_to_tid(rowid, &slot->tts_tid);
	slot->tts_tableOid = RelationGetRelid(rel);
	lt_builder_add(xt->builder, slot, version ? rowid | LE_ROWID_NEW_VERSION : rowid);
	xt->rows_inserted++;
	if (lt_builder_rows(xt->builder) >= lt_batch_rows ||
		lt_builder_bytes(xt->builder) >= (Size) 64 * 1024 * 1024)
		flush_builder(xt);
}

void
lt_xact_insert(Relation rel, TupleTableSlot *slot, CommandId cid)
{
	xact_insert(rel, slot, cid, 0, false);
}

/* A new version of a row, under the row's id: an in-place UPDATE's */
void
lt_xact_insert_version(Relation rel, TupleTableSlot *slot, CommandId cid, uint64 rowid)
{
	xact_insert(rel, slot, cid, rowid, true);
}

/*
 * The dataset version a write's snapshot reads, at which its command read
 * the committed rows it deletes: the commit deletes them where that version
 * has them (lt_xact_delete); 0 for a snapshot that is not MVCC.
 */
uint64
lt_xact_read_version(Relation rel, Snapshot snapshot)
{
	LtXactTable *xt = lt_xact_table(rel, true);

	if (snapshot == NULL || !IsMVCCSnapshot(snapshot))
		return 0;
	if (xt->read_snapshot != snapshot || xt->read_xmin != snapshot->xmin ||
		xt->read_curcid != snapshot->curcid ||
		xt->read_completion != snapshot->snapXactCompletionCount || xt->read_version == 0)
	{
		xt->read_version = lt_snapshot_version(rel, lt_table(rel), snapshot);
		xt->read_snapshot = snapshot;
		xt->read_xmin = snapshot->xmin;
		xt->read_curcid = snapshot->curcid;
		xt->read_completion = snapshot->snapXactCompletionCount;
	}
	return xt->read_version;
}

/* The rows the running command deleted, to tell a second delete of one */
static bool
cmd_deleted(LtXactTable *xt, uint64 rowid, CommandId cid)
{
	if (xt->cmd_deleted == NULL || xt->cmd_cid != cid)
		return false;
	return hash_search(xt->cmd_deleted, &rowid, HASH_FIND, NULL) != NULL;
}

static void
cmd_note_deleted(LtXactTable *xt, uint64 rowid, CommandId cid)
{
	if (xt->cmd_deleted == NULL || xt->cmd_cid != cid)
	{
		HASHCTL		ctl;

		if (xt->cmd_deleted)
			hash_destroy(xt->cmd_deleted);
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint64);
		ctl.entrysize = sizeof(uint64);
		ctl.hcxt = TopTransactionContext;
		xt->cmd_deleted = hash_create("lance_tam rows deleted", 1024, &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		xt->cmd_cid = cid;
	}
	(void) hash_search(xt->cmd_deleted, &rowid, HASH_ENTER, NULL);
}

/*
 * LE_WRITE_ROW_DELETED with the command that deleted it, when this
 * transaction's command "cid" or a later one -- a trigger's, under it --
 * deleted the row the command sees; else LE_WRITE_ROW_NONE.  A deletion by
 * an earlier command took a version the command does not see: the row
 * updated in place by it keeps its TID for the version it made.
 */
int
lt_xact_row_state(Relation rel, uint64 rowid, CommandId cid, CommandId *dcid)
{
	LtXactTable *xt = lt_xact_table_by_relid(RelationGetRelid(rel));

	if (xt == NULL)
		return LE_WRITE_ROW_NONE;
	if (xt->cmd_deleted && xt->cmd_cid >= cid &&
		hash_search(xt->cmd_deleted, &rowid, HASH_FIND, NULL))
	{
		*dcid = xt->cmd_cid;
		return LE_WRITE_ROW_DELETED;
	}
	return LE_WRITE_ROW_NONE;
}

/* Whether the transaction inserted the row */
bool
lt_xact_row_is_own(Oid relid, uint64 rowid)
{
	LtXactTable *xt = lt_xact_table_by_relid(relid);
	int32		state = LE_WRITE_ROW_NONE;
	int64		state_cid;
	LeError		err = {0};

	if (xt == NULL || xt->write == NULL)
		return false;
	if (xt->builder)
	{
		const uint64 *ids = lt_builder_rowids(xt->builder);

		for (int64 i = 0; i < lt_builder_rows(xt->builder); i++)
			if ((ids[i] & ~LE_ROWID_NEW_VERSION) == rowid)
				return true;
	}
	/* the row's newest version: an in-place update's is the transaction's */
	if (lt_api()->write_row_state(xt->write, rowid, -1, &state, &state_cid, &err) != LE_OK)
		lt_raise(&err, "reading a row's state");
	return state == LE_WRITE_ROW_APPENDED;
}

/*
 * A row deleted, the version of it the command sees; "version" is the
 * dataset version the command read the table at, 0 when unknown.
 */
void
lt_xact_delete(Relation rel, uint64 rowid, CommandId cid, uint64 version)
{
	LtXactTable *xt = lt_xact_table(rel, true);
	SubTransactionId subxid = GetCurrentSubTransactionId();

	lt_xact_write(rel, xt);
	if (cmd_deleted(xt, rowid, cid))
		return;
	/* the row may still be in the batch being formed */
	flush_builder(xt);
	if (xt->ndeletes > 0 &&
		(xt->delete_cid != cid || xt->delete_subxid != subxid || xt->delete_version != version))
		flush_deletes(xt);
	if (xt->ndeletes >= xt->deletes_cap)
	{
		int			cap = Max(1024, xt->deletes_cap * 2);

		if (xt->deletes == NULL)
			xt->deletes = MemoryContextAlloc(TopTransactionContext, sizeof(uint64) * cap);
		else
			xt->deletes = repalloc(xt->deletes, sizeof(uint64) * cap);
		xt->deletes_cap = cap;
	}
	xt->deletes[xt->ndeletes++] = rowid;
	xt->delete_cid = cid;
	xt->delete_subxid = subxid;
	xt->delete_version = version;
	xt->rows_deleted++;
	cmd_note_deleted(xt, rowid, cid);
	if (xt->ndeletes >= DELETE_FLUSH_ROWS)
		flush_deletes(xt);
}

void
lt_xact_note_created(Oid relid, const char *uri, const char *storage_json, bool managed)
{
	LtXactTable *xt = xact_table_enter(relid, uri, storage_json, managed);

	xt->created = true;
	xt->created_subxid = GetCurrentSubTransactionId();
}

void
lt_xact_note_dropped(Oid relid, const char *uri, const char *storage_json, bool managed)
{
	LtXactTable *xt = xact_table_enter(relid, uri, storage_json, managed);

	xt->dropped = true;
	xt->dropped_subxid = GetCurrentSubTransactionId();
}

/* The properties each version this transaction commits is tagged with */
char *
lt_xact_properties(Oid relid)
{
	StringInfoData buf;
	FullTransactionId fxid = GetTopFullTransactionId();

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "{\"pg.system_identifier\":\"" UINT64_FORMAT "\","
					 "\"pg.database\":\"%u\",\"pg.relation\":\"%u\","
					 "\"pg.xid\":\"" UINT64_FORMAT "\",\"pg.engine\":\"lance_tam\"}",
					 GetSystemIdentifier(), MyDatabaseId, relid,
					 U64FromFullTransactionId(fxid));
	return buf.data;
}

/* ------------------------------------------------------------------------
 * Commit
 * ------------------------------------------------------------------------
 */

/* A string member of a jsonb object, or NULL */
static char *
jsonb_member(JsonbContainer *obj, const char *key)
{
	JsonbValue	k;
	JsonbValue *v;

	k.type = jbvString;
	k.val.string.val = (char *) key;
	k.val.string.len = strlen(key);
	v = findJsonbValueFromContainer(obj, JB_FOBJECT, &k);
	if (v == NULL || v->type != jbvString)
		return NULL;
	return pnstrdup(v->val.string.val, v->val.string.len);
}

/*
 * Whether a version after the catalog's was committed to Lance by a
 * transaction of this server, for this table: that transaction aborted,
 * since a committed one would have set the catalog to it, and its version
 * must be undone.
 */
static bool
aborted_versions_after(Oid relid, const char *uri, const char *storage_json, uint64 version)
{
	LeError		err = {0};
	char	   *json = NULL;
	Jsonb	   *jb;
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken tok;
	char		sysid[32];
	char		db[16];
	char		rel[16];
	bool		found = false;

	if (lt_api()->versions(uri, storage_json, version, &json, &err) != LE_OK)
		lt_raise(&err, "reading a lance table's newer versions");
	json = lt_take_string(json);
	snprintf(sysid, sizeof(sysid), UINT64_FORMAT, GetSystemIdentifier());
	snprintf(db, sizeof(db), "%u", MyDatabaseId);
	snprintf(rel, sizeof(rel), "%u", relid);

	jb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json)));
	it = JsonbIteratorInit(&jb->root);
	while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		if (tok == WJB_ELEM && v.type == jbvBinary)
		{
			JsonbValue	k;
			JsonbValue *props;

			k.type = jbvString;
			k.val.string.val = "properties";
			k.val.string.len = strlen("properties");
			props = findJsonbValueFromContainer(v.val.binary.data, JB_FOBJECT, &k);
			if (props && props->type == jbvBinary)
			{
				char	   *s = jsonb_member(props->val.binary.data, "pg.system_identifier");
				char	   *d = jsonb_member(props->val.binary.data, "pg.database");
				char	   *r = jsonb_member(props->val.binary.data, "pg.relation");

				if (s && d && r && strcmp(s, sysid) == 0 && strcmp(d, db) == 0 &&
					strcmp(r, rel) == 0)
					found = true;
			}
		}
	}
	return found;
}

/*
 * The version a commit -- a writer's, VACUUM's -- builds on, under the
 * table's commit lock: the catalog's, after any version a transaction of
 * this server committed to Lance and then aborted has been undone.
 */
uint64
lt_commit_base(Oid relid, const char *uri, const char *storage_json, uint64 catalog_version,
			   const char *props)
{
	const LeApi *api = lt_api();
	LeError		err = {0};
	uint64		latest;

	if (api->latest_version(uri, storage_json, &latest, &err) != LE_OK)
		lt_raise(&err, "reading a lance table's latest version");
	if (latest > catalog_version && aborted_versions_after(relid, uri, storage_json, catalog_version))
	{
		uint64		restored;

		if (api->restore(uri, storage_json, catalog_version, props, &restored, &err) != LE_OK)
			lt_raise(&err, "undoing the versions of an aborted transaction");
		ereport(DEBUG1,
				(errmsg("lance_tam: restored version " UINT64_FORMAT " of \"%s\" as " UINT64_FORMAT,
						catalog_version, uri, restored)));
		return restored;
	}
	return catalog_version;
}

static void
commit_table(LtXactTable *xt)
{
	const LeApi *api = lt_api();
	LeError		err = {0};
	uint64		catalog_version;
	uint64		new_version = 0;
	LeDataset  *base = NULL;
	char	   *props;
	int			rc;

	lt_xact_flush(xt);
	if (!api->write_has_changes(xt->write))
		return;

	lt_lock_table_commit(xt->relid);
	props = lt_xact_properties(xt->relid);
	xt->props = MemoryContextStrdup(TopTransactionContext, props);
	catalog_version = lt_catalog_version(xt->relid, NULL);
	/* an abort restores the version before the transaction's first */
	if (!xt->published)
		xt->version_before = catalog_version;
	catalog_version = lt_commit_base(xt->relid, xt->uri, xt->storage_json, catalog_version, props);
	if (api->dataset_open(xt->uri, xt->storage_json, (int64) catalog_version, &base, &err) != LE_OK)
		lt_raise(&err, "opening a lance table's latest version");

	/*
	 * Once the engine has begun to commit, the commit runs to its end: an
	 * interrupt cutting it short would leave unknown whether a version was
	 * published.
	 */
	HOLD_INTERRUPTS();
	rc = api->write_commit(xt->write, base, props, &new_version, &err);
	RESUME_INTERRUPTS();
	api->dataset_close(base);
	if (rc != LE_OK)
		lt_raise(&err, "committing a lance table");

	if (!xt->published)
		xt->version_before = catalog_version;
	xt->published = true;
	xt->published_version = new_version;
	lt_catalog_set_version(xt->relid, new_version);

	/* the rows are the version's now: a read from here on reads them there */
	api->write_free(xt->write, false);
	xt->write = NULL;
	xt->rows_inserted = xt->rows_deleted = 0;
	xt->truncated = false;

	/* the tests' failures between Lance's commit and the server's */
	if (lt_debug_fail_after_commit == 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance.debug_fail_after_commit: failing after version " UINT64_FORMAT,
						new_version)));
	if (lt_debug_fail_after_commit == 2)
		abort();
}

static void
pre_commit(void)
{
	HASH_SEQ_STATUS status;
	LtXactTable *xt;

	xact_committing = true;
	if (xact_tables == NULL)
		return;
	/* the catalog's rows are a heap table's, updated under a snapshot */
	PushActiveSnapshot(GetTransactionSnapshot());
	hash_seq_init(&status, xact_tables);
	while ((xt = hash_seq_search(&status)) != NULL)
	{
		if (xt->write && !xt->dropped)
			commit_table(xt);
	}
	PopActiveSnapshot();
}

/* The names of a dataset's columns */
static List *
dataset_column_names(LeDataset *ds)
{
	struct ArrowSchema schema;
	LeError		err = {0};
	List	   *names = NIL;

	memset(&schema, 0, sizeof(schema));
	if (lt_api()->dataset_schema(ds, &schema, &err) != LE_OK)
		lt_raise(&err, "reading a lance table's schema");
	for (int64 i = 0; i < schema.n_children; i++)
		if (schema.children[i]->name)
			names = lappend(names, pstrdup(schema.children[i]->name));
	if (schema.release)
		schema.release(&schema);
	return names;
}

static bool
name_taken(List *names, const char *name)
{
	ListCell   *lc;

	foreach(lc, names)
		if (strcmp((char *) lfirst(lc), name) == 0)
			return true;
	return false;
}

/*
 * Before a change of the dataset's columns: the transaction's changes to
 * the table committed first, as the version before it, so that its later
 * rows are written in the new columns -- refused in a subtransaction,
 * whose rollback would take the catalog's record of that version along --
 * and the version the change is made on, under the table's commit lock.
 */
static uint64
column_change_base(Relation rel, LtXactTable *xt, LtTable *t, const char *what, char **props)
{
	const LeApi *api = lt_api();
	Oid			relid = RelationGetRelid(rel);
	uint64		catalog_version;

	if (!t->writable)
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("lance table \"%s\" is read-only", RelationGetRelationName(rel))));
	if (xt->write)
	{
		lt_xact_flush(xt);
		if (api->write_has_changes(xt->write))
		{
			if (GetCurrentTransactionNestLevel() > 1)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot %s lance table \"%s\" in a subtransaction after changing its rows",
								what, RelationGetRelationName(rel)),
						 errhint("Change the columns before the transaction's changes, or outside the savepoint.")));
			PushActiveSnapshot(GetTransactionSnapshot());
			commit_table(xt);
			PopActiveSnapshot();
		}
		if (xt->write)
		{
			api->write_free(xt->write, true);
			xt->write = NULL;
		}
	}
	if (xt->builder)
	{
		lt_builder_free(xt->builder);
		xt->builder = NULL;
	}

	lt_lock_table_commit(relid);
	*props = lt_xact_properties(relid);
	xt->props = MemoryContextStrdup(TopTransactionContext, *props);
	catalog_version = lt_catalog_version(relid, NULL);
	if (!xt->published)
		xt->version_before = catalog_version;
	return lt_commit_base(relid, t->uri, t->storage_json, catalog_version, *props);
}

/* The change of the columns committed: the catalog row names its version */
static void
column_change_done(LtXactTable *xt, Oid relid, uint64 base, uint64 new_version, List *columns)
{
	if (!xt->published)
		xt->version_before = base;
	xt->published = true;
	xt->published_version = new_version;
	lt_catalog_set_version(relid, new_version);
	lt_catalog_set_columns(relid, columns);
}

/*
 * ALTER TABLE ... DROP COLUMN: the columns dropped from the dataset too,
 * as a version of their own -- the manifest's change alone, the data
 * staying until compaction -- so that the dataset needs no value of them,
 * a NOT NULL column's above all, and other readers no longer see them.
 */
void
lt_xact_drop_columns(Oid relid, List *attnums)
{
	Relation	rel = table_open(relid, NoLock);
	TupleDesc	desc = RelationGetDescr(rel);
	const LeApi *api = lt_api();
	LtTable    *t = lt_table(rel);
	LtXactTable *xt = lt_xact_table(rel, true);
	const char **names = palloc0(sizeof(char *) * Max(list_length(attnums), 1));
	List	   *columns = NIL;
	int			n = 0;
	int			left = 0;
	LeDataset  *ds;
	LeError		err = {0};
	uint64		base;
	uint64		new_version = 0;
	char	   *props;
	int			rc;
	ListCell   *lc;

	foreach(lc, attnums)
	{
		int			i = lfirst_int(lc) - 1;

		if (i < t->natts && t->columns[i] != NULL && TupleDescAttr(desc, i)->attisdropped)
			names[n++] = t->columns[i];
	}
	/* a read-only table's dataset is not the server's to change */
	if (n == 0 || !t->writable)
	{
		table_close(rel, NoLock);
		return;
	}
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		const char *name = i < t->natts && t->columns[i] && !att->attisdropped ? t->columns[i] : "";

		if (name[0])
			left++;
		columns = lappend(columns, pstrdup(name));
	}
	if (left == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop the last column of lance table \"%s\"", RelationGetRelationName(rel)),
				 errdetail("A Lance dataset has at least one column.")));

	base = column_change_base(rel, xt, t, "drop a column of", &props);
	ds = lt_open_dataset(t, base);
	HOLD_INTERRUPTS();
	rc = api->drop_columns(ds, names, n, props, &new_version, &err);
	RESUME_INTERRUPTS();
	api->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "dropping columns of a lance table");
	column_change_done(xt, relid, base, new_version, columns);
	table_close(rel, NoLock);
}

/*
 * ALTER TABLE ... ADD COLUMN: the columns PostgreSQL added become the
 * dataset's, as a version of their own committed at once, each row the
 * table has taking the value PostgreSQL gives the rows that predate a
 * column -- the default it keeps as attmissingval, else NULL.  A column is
 * named as its attribute, unless the dataset has a column of that name, a
 * dropped attribute's.  The transaction's changes to the table come
 * first, committed as the version before it; in a subtransaction they
 * cannot be, as its abort would take the catalog's record of them along.
 */
void
lt_xact_add_columns(Oid relid, List *attnums)
{
	Relation	rel = table_open(relid, NoLock);
	TupleDesc	desc = RelationGetDescr(rel);
	const LeApi *api = lt_api();
	LtTable    *t = lt_table(rel);
	LtXactTable *xt = lt_xact_table(rel, true);
	TupleDesc	sub;
	LtLayout   *layout;
	char	  **names;
	int		   *attidx;
	AttrNumber *orig;
	List	   *existing;
	List	   *columns = NIL;
	int			n = 0;
	bool		any_value = false;
	struct ArrowSchema fields;
	struct ArrowArray values;
	LeDataset  *ds = NULL;
	LeError		err = {0};
	uint64		base;
	uint64		new_version = 0;
	char	   *props;
	int			rc;
	ListCell   *lc;

	/* the attributes PostgreSQL stores: a virtual generated one it computes */
	foreach(lc, attnums)
	{
		Form_pg_attribute att = TupleDescAttr(desc, lfirst_int(lc) - 1);

		if (!att->attisdropped && att->attgenerated != ATTRIBUTE_GENERATED_VIRTUAL)
			n++;
	}
	if (n == 0)
	{
		table_close(rel, NoLock);
		return;
	}

	base = column_change_base(rel, xt, t, "add a column to", &props);
	ds = lt_open_dataset(t, base);
	existing = dataset_column_names(ds);

	/* the new attributes, alone, and their columns' names */
	sub = CreateTemplateTupleDesc(n);
	names = palloc0(sizeof(char *) * n);
	attidx = palloc0(sizeof(int) * n);
	orig = palloc0(sizeof(AttrNumber) * n);
	n = 0;
	foreach(lc, attnums)
	{
		AttrNumber	attnum = (AttrNumber) lfirst_int(lc);
		Form_pg_attribute att = TupleDescAttr(desc, attnum - 1);
		char	   *name = pstrdup(NameStr(att->attname));

		if (att->attisdropped || att->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
			continue;
		for (int k = 2; name_taken(existing, name); k++)
			name = psprintf("%s_%d", NameStr(att->attname), k);
		existing = lappend(existing, name);
		TupleDescCopyEntry(sub, n + 1, desc, attnum);
		names[n] = name;
		attidx[n] = n;
		orig[n] = attnum;
		n++;
	}
	TupleDescFinalize(sub);
	layout = lt_layout_build_desc(sub, names, n, CurrentMemoryContext);

	/* the value each row has: one row of them, unless every one is NULL */
	memset(&fields, 0, sizeof(fields));
	memset(&values, 0, sizeof(values));
	{
		TupleTableSlot *slot = MakeSingleTupleTableSlot(sub, &TTSOpsVirtual);

		ExecClearTuple(slot);
		for (int i = 0; i < n; i++)
		{
			slot->tts_values[i] = getmissingattr(desc, orig[i], &slot->tts_isnull[i]);
			any_value |= !slot->tts_isnull[i];
		}
		ExecStoreVirtualTuple(slot);
		if (any_value)
		{
			LtBuilder  *b = lt_builder_create(layout);

			lt_builder_add(b, slot, 0);
			lt_builder_export(b, &values, &fields);
			lt_builder_free(b);
		}
		else
			lt_export_schema(layout, n, attidx, &fields);
		ExecDropSingleTupleTableSlot(slot);
	}
	PG_TRY();
	{
		lt_schema_set_encoding(&fields, t->options, false);
	}
	PG_CATCH();
	{
		if (values.release)
			values.release(&values);
		if (fields.release)
			fields.release(&fields);
		api->dataset_close(ds);
		PG_RE_THROW();
	}
	PG_END_TRY();

	HOLD_INTERRUPTS();
	rc = api->add_columns(ds, &fields, any_value ? &values : NULL, props, &new_version, &err);
	RESUME_INTERRUPTS();
	if (values.release)
		values.release(&values);
	if (fields.release)
		fields.release(&fields);
	api->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "adding columns to a lance table");

	/* the table's columns: its attributes', the new ones' named here */
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		const char *name = i < t->natts && t->columns[i] ? t->columns[i] : "";

		for (int j = 0; j < n; j++)
			if (orig[j] == att->attnum)
				name = names[j];
		columns = lappend(columns, pstrdup(att->attisdropped ? "" : name));
	}
	column_change_done(xt, relid, base, new_version, columns);
	table_close(rel, NoLock);
}

/*
 * The rows a snapshot of the table sees, as far as the planner needs to
 * know: the committed version's, and what the transaction's changes add
 * and take -- a table filled in the transaction that made it is not
 * empty.  Subtransactions rolled back are not subtracted.
 */
double
lt_xact_rows(Oid relid, double committed)
{
	LtXactTable *xt = lt_xact_table_by_relid(relid);
	double		rows = committed;

	if (xt == NULL || xt->write == NULL)
		return rows;
	if (xt->truncated)
		rows = 0;
	rows += (double) xt->rows_inserted - (double) xt->rows_deleted;
	return Max(rows, 0);
}

bool
lt_xact_committing(void)
{
	return xact_committing;
}

/*
 * ON COMMIT DELETE ROWS truncates a table after XACT_EVENT_PRE_COMMIT has
 * committed the transaction's rows of it to Lance: the truncation is one
 * more version, committed at once.  A table the transaction committed
 * nothing to is empty already, as such a table is at every transaction's
 * start.
 */
void
lt_xact_truncate_committed(Relation rel)
{
	LtXactTable *xt = lt_xact_table_by_relid(RelationGetRelid(rel));
	const LeApi *api = lt_api();
	LtTable    *t;
	LeDataset  *base = NULL;
	LeWrite    *w = NULL;
	LeWriteSpec spec;
	LeError		err = {0};
	uint64		new_version = 0;
	int			rc;

	if (xt == NULL || !xt->published || xt->dropped)
		return;
	t = lt_table(rel);
	if (api->dataset_open(xt->uri, xt->storage_json, (int64) xt->published_version, &base, &err) != LE_OK)
		lt_raise(&err, "opening a lance table's latest version");
	memset(&spec, 0, sizeof(spec));
	spec.size = sizeof(spec);
	spec.storage_version = t->storage_version;
	rc = api->write_begin(xt->uri, xt->storage_json, base, NULL, &spec, &w, &err);
	if (rc == LE_OK)
		rc = api->write_truncate(w, (int64) GetCurrentCommandId(false),
								 GetCurrentSubTransactionId(), &err);
	if (rc == LE_OK)
	{
		HOLD_INTERRUPTS();
		rc = api->write_commit(w, base, xt->props, &new_version, &err);
		RESUME_INTERRUPTS();
	}
	if (w)
		api->write_free(w, rc != LE_OK);
	api->dataset_close(base);
	if (rc != LE_OK)
		lt_raise(&err, "truncating a lance table at commit");

	xt->published_version = new_version;
	PushActiveSnapshot(GetTransactionSnapshot());
	lt_catalog_set_version(xt->relid, new_version);
	PopActiveSnapshot();
}

static void
end_of_xact(bool commit)
{
	HASH_SEQ_STATUS status;
	LtXactTable *xt;
	const LeApi *api;

	xact_committing = false;
	lt_scan_reset_readers();
	if (xact_tables == NULL)
		return;
	api = lt_engine_loaded() ? lt_api() : NULL;
	hash_seq_init(&status, xact_tables);
	while ((xt = hash_seq_search(&status)) != NULL)
	{
		LeError		err = {0};

		if (xt->builder)
			lt_builder_free(xt->builder);
		xt->builder = NULL;
		if (api == NULL)
			continue;
		if (xt->write)
		{
			/* abort: the fragments and index files nothing committed */
			api->write_free(xt->write, !commit);
			xt->write = NULL;
		}
		if (!commit && xt->published)
		{
			uint64		restored;

			if (api->restore(xt->uri, xt->storage_json, xt->version_before, xt->props, &restored, &err) != LE_OK)
				lt_warn(&err, "could not undo the Lance version an aborted transaction committed");
		}
		if (xt->managed &&
			((commit && xt->dropped) || (!commit && xt->created)))
		{
			if (api->remove_dataset(xt->uri, xt->storage_json, &err) != LE_OK)
				lt_warn(&err, "could not remove a lance table's dataset");
		}
	}
	xact_tables = NULL;
}

static void
lt_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
			pre_commit();
			break;
		case XACT_EVENT_PRE_PREPARE:
			if (xact_tables != NULL)
			{
				HASH_SEQ_STATUS status;
				LtXactTable *xt;

				hash_seq_init(&status, xact_tables);
				while ((xt = hash_seq_search(&status)) != NULL)
				{
					if (xt->write || xt->created || xt->dropped)
					{
						hash_seq_term(&status);
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot PREPARE a transaction that changed a lance table")));
					}
				}
			}
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			end_of_xact(true);
			break;
		case XACT_EVENT_PREPARE:
			/* nothing of a lance table's to keep: the state is in the transaction's memory */
			end_of_xact(true);
			break;
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			end_of_xact(false);
			break;
		default:
			break;
	}
}

static void
lt_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
					SubTransactionId parentSubid, void *arg)
{
	HASH_SEQ_STATUS status;
	LtXactTable *xt;
	const LeApi *api;

	if (xact_tables == NULL)
		return;
	if (event != SUBXACT_EVENT_PRE_COMMIT_SUB && event != SUBXACT_EVENT_COMMIT_SUB &&
		event != SUBXACT_EVENT_ABORT_SUB)
		return;
	api = lt_engine_loaded() ? lt_api() : NULL;
	hash_seq_init(&status, xact_tables);
	while ((xt = hash_seq_search(&status)) != NULL)
	{
		LeError		err = {0};

		if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		{
			/* the subtransaction's rows to the engine, while errors may raise */
			lt_xact_flush(xt);
			continue;
		}
		if (event == SUBXACT_EVENT_ABORT_SUB)
		{
			/* the rows the subtransaction formed and deleted go with it */
			if (xt->builder && xt->builder_subxid == mySubid)
				lt_builder_reset(xt->builder);
			if (xt->ndeletes > 0 && xt->delete_subxid == mySubid)
				xt->ndeletes = 0;
			if (xt->cmd_deleted)
			{
				hash_destroy(xt->cmd_deleted);
				xt->cmd_deleted = NULL;
			}
		}
		if (xt->write && api &&
			api->write_subxact(xt->write, mySubid, parentSubid,
							   event == SUBXACT_EVENT_COMMIT_SUB, &err) != LE_OK)
			lt_warn(&err, "ending a subtransaction's changes to a lance table");
		if (xt->created && xt->created_subxid == mySubid)
		{
			if (event == SUBXACT_EVENT_COMMIT_SUB)
				xt->created_subxid = parentSubid;
			else
			{
				LeError		e2 = {0};

				if (api && xt->managed && api->remove_dataset(xt->uri, xt->storage_json, &e2) != LE_OK)
					lt_warn(&e2, "could not remove a lance table's dataset");
				xt->created = false;
			}
		}
		if (xt->dropped && xt->dropped_subxid == mySubid)
		{
			if (event == SUBXACT_EVENT_COMMIT_SUB)
				xt->dropped_subxid = parentSubid;
			else
				xt->dropped = false;
		}
	}
}
