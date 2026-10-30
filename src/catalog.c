/*-------------------------------------------------------------------------
 *
 * catalog.c
 *	  lance.tables, the extension's catalog (docs/design.md §2.6): a row a
 *	  table, naming its dataset and the version of it the table is.  The
 *	  row is an ordinary heap row, so a snapshot sees the version the last
 *	  transaction visible to it committed, and a transaction that aborts
 *	  leaves the version its predecessor committed.  A writer updates it at
 *	  commit, holding the table's commit lock (lt_lock_table_commit) until
 *	  its transaction ends, so that writers commit one after another and
 *	  each on the last one's version.
 *
 *	  The fixed part of a row -- the dataset, its options, its columns -- is
 *	  kept per backend until the relation changes, and with it the fragments
 *	  and the Lance indices of the version the planner last asked about,
 *	  which never change for that version (lt_version_info).  Storage
 *	  options merge,
 *	  in this order: the storage server's (a FOREIGN SERVER of the wrapper
 *	  lance_storage), the table owner's user mapping for it or PUBLIC's, and
 *	  the table's own options that are not the extension's.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "storage/lmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "lance_tam.h"

#define Natts_tables			9
#define Anum_tables_relid		1
#define Anum_tables_uri			2
#define Anum_tables_version		3
#define Anum_tables_managed		4
#define Anum_tables_writable	5
#define Anum_tables_follow		6
#define Anum_tables_columns		7
#define Anum_tables_options		8
#define Anum_tables_server		9

/* The extension's options of a table: the rest are storage options */
static const char *const table_option_names[] = {
	"uri", "server", "managed", "read_only", "follow", "storage_version",
	"max_rows_per_file", "max_rows_per_group", "buffer_rows",
	"compression", "compression_level", "column_encoding", NULL
};

static bool
is_table_option(const char *name)
{
	for (int i = 0; table_option_names[i]; i++)
		if (strcmp(table_option_names[i], name) == 0)
			return true;
	return false;
}

Oid
lt_catalog_relid(void)
{
	Oid			nsp = get_namespace_oid("lance", true);
	Oid			relid = OidIsValid(nsp) ? get_relname_relid("tables", nsp) : InvalidOid;

	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("extension \"lance_tam\" is not installed in this database"),
				 errhint("CREATE EXTENSION lance_tam;")));
	return relid;
}

static Oid
catalog_index_relid(void)
{
	Oid			nsp = get_namespace_oid("lance", false);
	Oid			relid = get_relname_relid("tables_pkey", nsp);

	if (!OidIsValid(relid))
		elog(ERROR, "lance_tam: lance.tables_pkey is missing");
	return relid;
}

static HeapTuple
catalog_fetch(Relation catrel, Oid relid, Snapshot snapshot)
{
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tup;

	ScanKeyInit(&key, Anum_tables_relid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relid));
	scan = systable_beginscan(catrel, catalog_index_relid(), true, snapshot, 1, &key);
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
		tup = heap_copytuple(tup);
	systable_endscan(scan);
	return tup;
}

uint64
lt_catalog_version(Oid relid, Snapshot snapshot)
{
	Relation	catrel = table_open(lt_catalog_relid(), AccessShareLock);
	HeapTuple	tup;
	bool		isnull;
	uint64		version = 0;
	bool		registered = false;

	/*
	 * A snapshot that is not MVCC -- SnapshotAny fetching the row a query's
	 * scan read -- reads the version of the query's own snapshot when one
	 * is active, else the latest.
	 */
	if (snapshot != NULL && !IsMVCCSnapshot(snapshot) && snapshot == SnapshotAny &&
		ActiveSnapshotSet())
		snapshot = GetActiveSnapshot();
	if (snapshot == NULL || !IsMVCCSnapshot(snapshot))
	{
		snapshot = RegisterSnapshot(GetLatestSnapshot());
		registered = true;
	}
	tup = catalog_fetch(catrel, relid, snapshot);
	if (registered)
		UnregisterSnapshot(snapshot);
	if (HeapTupleIsValid(tup))
	{
		Datum		d = heap_getattr(tup, Anum_tables_version, RelationGetDescr(catrel), &isnull);

		version = isnull ? 0 : (uint64) DatumGetInt64(d);
		heap_freetuple(tup);
	}
	else
	{
		/*
		 * Not visible to the snapshot: a table created by a transaction the
		 * snapshot does not see -- its row and its relation are invisible
		 * together, so this is a table the caller should not see either --
		 * or one this transaction made, after the snapshot.
		 */
		Snapshot	self = SnapshotSelf;

		tup = catalog_fetch(catrel, relid, self);
		if (HeapTupleIsValid(tup))
		{
			Datum		d = heap_getattr(tup, Anum_tables_version, RelationGetDescr(catrel), &isnull);

			version = isnull ? 0 : (uint64) DatumGetInt64(d);
			heap_freetuple(tup);
		}
		else
		{
			table_close(catrel, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("relation %u has no row in lance.tables", relid)));
		}
	}
	table_close(catrel, AccessShareLock);
	return version;
}

/* ------------------------------------------------------------------------
 * The fixed part of a table's row, cached
 * ------------------------------------------------------------------------
 */

typedef struct TableEntry
{
	Oid			relid;
	LtTable    *table;
	MemoryContext cxt;
	/* what the planner asked last of a version (lt_version_info), in a child of cxt */
	LtVersionInfo *vinfo;
	MemoryContext vinfo_cxt;
	/* the columns an in-place UPDATE may not change, in cxt */
	bool		blocking_known;
	Bitmapset  *blocking;
} TableEntry;

static HTAB *tables = NULL;

static void
table_entry_drop(TableEntry *e)
{
	if (e->cxt)
		MemoryContextDelete(e->cxt);
	e->cxt = NULL;
	e->table = NULL;
	e->vinfo = NULL;
	e->vinfo_cxt = NULL;
	e->blocking_known = false;
	e->blocking = NULL;
}

void
lt_table_invalidate(Oid relid)
{
	TableEntry *e;

	if (tables == NULL)
		return;
	e = hash_search(tables, &relid, HASH_FIND, NULL);
	if (e)
	{
		table_entry_drop(e);
		hash_search(tables, &relid, HASH_REMOVE, NULL);
	}
}

static void
table_invalidate_cb(Datum arg, Oid relid)
{
	if (tables == NULL)
		return;
	if (OidIsValid(relid))
		lt_table_invalidate(relid);
	else
	{
		HASH_SEQ_STATUS status;
		TableEntry *e;

		hash_seq_init(&status, tables);
		while ((e = hash_seq_search(&status)) != NULL)
		{
			table_entry_drop(e);
			hash_search(tables, &e->relid, HASH_REMOVE, NULL);
		}
	}
}

static List *
text_array_list(Datum d)
{
	ArrayType  *arr = DatumGetArrayTypeP(d);
	Datum	   *elems;
	bool	   *nulls;
	int			n;
	List	   *out = NIL;

	deconstruct_array_builtin(arr, TEXTOID, &elems, &nulls, &n);
	for (int i = 0; i < n; i++)
		out = lappend(out, nulls[i] ? NULL : TextDatumGetCString(elems[i]));
	return out;
}

/* "key=value" strings, as DefElems */
static List *
options_from_strings(List *strings)
{
	List	   *defs = NIL;
	ListCell   *lc;

	foreach(lc, strings)
	{
		char	   *s = lfirst(lc);
		char	   *eq;

		if (s == NULL)
			continue;
		eq = strchr(s, '=');
		if (eq == NULL)
			defs = lappend(defs, makeDefElem(pstrdup(s), NULL, -1));
		else
		{
			char	   *name = pnstrdup(s, eq - s);

			defs = lappend(defs, makeDefElem(name, (Node *) makeString(pstrdup(eq + 1)), -1));
		}
	}
	return defs;
}

bool
lt_option_bool(List *options, const char *name, bool dflt)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *d = lfirst(lc);

		if (strcmp(d->defname, name) == 0)
			return defGetBoolean(d);
	}
	return dflt;
}

char *
lt_option_string(List *options, const char *name)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *d = lfirst(lc);

		if (strcmp(d->defname, name) == 0)
			return defGetString(d);
	}
	return NULL;
}

int64
lt_option_int(List *options, const char *name, int64 dflt)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *d = lfirst(lc);

		if (strcmp(d->defname, name) == 0)
		{
			/* the options are kept as text, as a WITH clause's are */
			char	   *str = defGetString(d);
			char	   *end;
			int64		v;

			errno = 0;
			v = strtoi64(str, &end, 10);
			if (errno != 0 || end == str || *end != '\0' || v <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("option \"%s\" of a lance table must be a positive integer, not \"%s\"",
								name, str)));
			return v;
		}
	}
	return dflt;
}

static void
json_add(StringInfo buf, bool *first, const char *key, const char *value)
{
	if (!*first)
		appendStringInfoChar(buf, ',');
	*first = false;
	escape_json(buf, key);
	appendStringInfoChar(buf, ':');
	escape_json(buf, value ? value : "");
}

static void
add_option_list(StringInfo buf, bool *first, List *options, bool skip_ours)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *d = lfirst(lc);

		if (skip_ours && is_table_option(d->defname))
			continue;
		json_add(buf, first, d->defname, d->arg ? defGetString(d) : "true");
	}
}

/*
 * The storage options of a table, as the JSON object the library takes.
 * Credentials belong in a user mapping, which only its owner and superusers
 * read; the options of a table are any user's to read.
 */
char *
lt_storage_json(List *options, const char *server, Oid owner)
{
	StringInfoData buf;
	bool		first = true;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');
	if (server && server[0])
	{
		ForeignServer *fs = GetForeignServerByName(server, false);
		ForeignDataWrapper *fdw = GetForeignDataWrapper(fs->fdwid);
		HeapTuple	tup;

		if (strcmp(fdw->fdwname, "lance_storage") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("server \"%s\" is not a server of the foreign-data wrapper lance_storage",
							server)));
		add_option_list(&buf, &first, fs->options, false);
		tup = SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(owner),
							  ObjectIdGetDatum(fs->serverid));
		if (!HeapTupleIsValid(tup))
			tup = SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(InvalidOid),
								  ObjectIdGetDatum(fs->serverid));
		if (HeapTupleIsValid(tup))
		{
			bool		isnull;
			Datum		d = SysCacheGetAttr(USERMAPPINGUSERSERVER, tup,
											Anum_pg_user_mapping_umoptions, &isnull);

			if (!isnull)
				add_option_list(&buf, &first, untransformRelOptions(d), false);
			ReleaseSysCache(tup);
		}
	}
	add_option_list(&buf, &first, options, true);
	appendStringInfoChar(&buf, '}');
	return buf.data;
}

static LtTable *
table_load(Oid relid, bool missing_ok, MemoryContext cxt)
{
	Relation	catrel = table_open(lt_catalog_relid(), AccessShareLock);
	HeapTuple	tup = catalog_fetch(catrel, relid, SnapshotSelf);
	TupleDesc	desc = RelationGetDescr(catrel);
	Datum		values[Natts_tables];
	bool		nulls[Natts_tables];
	LtTable    *t;
	MemoryContext old;
	List	   *columns;
	List	   *optstrings = NIL;
	ListCell   *lc;
	int			i;
	Oid			owner;

	if (!HeapTupleIsValid(tup))
	{
		table_close(catrel, AccessShareLock);
		if (missing_ok)
			return NULL;
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("relation \"%s\" is not a lance table", get_rel_name(relid) ? get_rel_name(relid) : "?")));
	}
	heap_deform_tuple(tup, desc, values, nulls);
	owner = InvalidOid;
	{
		HeapTuple	ctup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

		if (HeapTupleIsValid(ctup))
		{
			owner = ((Form_pg_class) GETSTRUCT(ctup))->relowner;
			ReleaseSysCache(ctup);
		}
	}

	old = MemoryContextSwitchTo(cxt);
	t = palloc0(sizeof(LtTable));
	t->relid = relid;
	t->uri = TextDatumGetCString(values[Anum_tables_uri - 1]);
	t->managed = DatumGetBool(values[Anum_tables_managed - 1]);
	t->writable = DatumGetBool(values[Anum_tables_writable - 1]);
	t->follow = DatumGetBool(values[Anum_tables_follow - 1]);
	t->server = nulls[Anum_tables_server - 1] ? NULL : TextDatumGetCString(values[Anum_tables_server - 1]);
	columns = text_array_list(values[Anum_tables_columns - 1]);
	t->natts = list_length(columns);
	t->columns = palloc0(sizeof(char *) * Max(t->natts, 1));
	i = 0;
	foreach(lc, columns)
	{
		char	   *c = lfirst(lc);

		t->columns[i++] = (c && c[0]) ? c : NULL;
	}
	if (!nulls[Anum_tables_options - 1])
		optstrings = text_array_list(values[Anum_tables_options - 1]);
	t->options = options_from_strings(optstrings);
	t->max_rows_per_file = (int) lt_option_int(t->options, "max_rows_per_file", 0);
	t->max_rows_per_group = (int) lt_option_int(t->options, "max_rows_per_group", 0);
	t->buffer_rows = lt_option_int(t->options, "buffer_rows", 0);
	t->storage_version = lt_option_string(t->options, "storage_version");
	t->storage_json = lt_storage_json(t->options, t->server, owner);
	MemoryContextSwitchTo(old);

	heap_freetuple(tup);
	table_close(catrel, AccessShareLock);
	return t;
}

LtTable *
lt_table_by_relid(Oid relid, bool missing_ok)
{
	TableEntry *e;
	bool		found;
	MemoryContext cxt;
	LtTable    *t;

	if (tables == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(TableEntry);
		ctl.hcxt = CacheMemoryContext;
		tables = hash_create("lance_tam tables", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		CacheRegisterRelcacheCallback(table_invalidate_cb, (Datum) 0);
	}
	e = hash_search(tables, &relid, HASH_FIND, NULL);
	if (e && e->table)
		return e->table;

	cxt = AllocSetContextCreate(CacheMemoryContext, "lance_tam table", ALLOCSET_SMALL_SIZES);
	PG_TRY();
	{
		t = table_load(relid, missing_ok, cxt);
	}
	PG_CATCH();
	{
		MemoryContextDelete(cxt);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (t == NULL)
	{
		MemoryContextDelete(cxt);
		return NULL;
	}
	e = hash_search(tables, &relid, HASH_ENTER, &found);
	if (found)
		table_entry_drop(e);
	e->table = t;
	e->cxt = cxt;
	e->vinfo = NULL;
	e->vinfo_cxt = NULL;
	e->blocking_known = false;
	e->blocking = NULL;
	return t;
}

/* A table's row, copied: an invalidation may free the cached one */
static LtTable *
table_copy(const LtTable *src)
{
	LtTable    *t = palloc(sizeof(LtTable));

	*t = *src;
	t->uri = pstrdup(src->uri);
	t->storage_json = pstrdup(src->storage_json);
	t->server = src->server ? pstrdup(src->server) : NULL;
	t->storage_version = src->storage_version ? pstrdup(src->storage_version) : NULL;
	t->columns = palloc0(sizeof(char *) * Max(src->natts, 1));
	for (int i = 0; i < src->natts; i++)
		t->columns[i] = src->columns[i] ? pstrdup(src->columns[i]) : NULL;
	t->options = copyObject(src->options);
	return t;
}

LtTable *
lt_table(Relation rel)
{
	return table_copy(lt_table_by_relid(RelationGetRelid(rel), false));
}

LtTable *
lt_table_copy_by_relid(Oid relid, bool missing_ok)
{
	LtTable    *t = lt_table_by_relid(relid, missing_ok);

	return t ? table_copy(t) : NULL;
}

/*
 * The columns an in-place UPDATE may not change (tableam.c), NULL for none:
 * every column an index reads -- its keys and included columns, its
 * expressions', its predicate's -- as heap's HOT-blocking columns are
 * (RelationGetIndexAttrBitmap), but a lance_index's, whose entries are
 * Lance's, and a summarizing index's.  Kept with the table's row: creating
 * or dropping an index invalidates the relation.
 */
Bitmapset *
lt_table_inplace_blocking(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	TableEntry *e;
	List	   *indexes;
	ListCell   *lc;
	Bitmapset  *attrs = NULL;

	(void) lt_table_by_relid(relid, false);
	e = hash_search(tables, &relid, HASH_FIND, NULL);
	if (e && e->table && e->blocking_known)
		return bms_copy(e->blocking);
	indexes = RelationGetIndexList(rel);
	foreach(lc, indexes)
	{
		Relation	index = index_open(lfirst_oid(lc), AccessShareLock);

		if (!lt_is_lance_index_am(index->rd_rel->relam) && !index->rd_indam->amsummarizing)
		{
			Form_pg_index form = index->rd_index;

			for (int i = 0; i < form->indnatts; i++)
			{
				if (form->indkey.values[i] != 0)
					attrs = bms_add_member(attrs, form->indkey.values[i] -
										   FirstLowInvalidHeapAttributeNumber);
			}
			pull_varattnos((Node *) RelationGetIndexExpressions(index), 1, &attrs);
			pull_varattnos((Node *) RelationGetIndexPredicate(index), 1, &attrs);
		}
		index_close(index, AccessShareLock);
	}
	list_free(indexes);

	/* an invalidation while the indexes were opened may have dropped the entry */
	e = hash_search(tables, &relid, HASH_FIND, NULL);
	if (e && e->table)
	{
		MemoryContext old = MemoryContextSwitchTo(e->cxt);

		e->blocking = bms_copy(attrs);
		e->blocking_known = true;
		MemoryContextSwitchTo(old);
	}
	return attrs;
}

/* ------------------------------------------------------------------------
 * A version's fragments and indices, kept with the table's row
 * ------------------------------------------------------------------------
 */

/* A member of a jsonb object, or NULL */
static JsonbValue *
json_member(JsonbContainer *obj, const char *key)
{
	JsonbValue	k;

	k.type = jbvString;
	k.val.string.val = (char *) key;
	k.val.string.len = strlen(key);
	return findJsonbValueFromContainer(obj, JB_FOBJECT, &k);
}

static double
json_number(JsonbContainer *obj, const char *key, double dflt)
{
	JsonbValue *v = json_member(obj, key);

	if (v == NULL || v->type != jbvNumeric)
		return dflt;
	return DatumGetFloat8(DirectFunctionCall1(numeric_float8, NumericGetDatum(v->val.numeric)));
}

/* Lance names an index's type by its details' message: BTreeIndexDetails */
static LtIndexKind
index_kind(const JsonbValue *type)
{
	if (type == NULL || type->type != jbvString)
		return LT_INDEX_OTHER;
	if (type->val.string.len >= 5 && pg_strncasecmp(type->val.string.val, "btree", 5) == 0)
		return LT_INDEX_BTREE;
	if (type->val.string.len >= 6 && pg_strncasecmp(type->val.string.val, "bitmap", 6) == 0)
		return LT_INDEX_BITMAP;
	if (type->val.string.len >= 9 && pg_strncasecmp(type->val.string.val, "labellist", 9) == 0)
		return LT_INDEX_LABEL_LIST;
	return LT_INDEX_OTHER;
}

/* The attribute a column of the dataset is, or 0 */
static AttrNumber
column_attnum(LtTable *t, const char *name)
{
	for (int a = 0; a < t->natts; a++)
	{
		if (t->columns[a] && strcmp(t->columns[a], name) == 0)
			return a + 1;
	}
	return 0;
}

/*
 * The attributes whose field is the timestamp[us] the server writes for a
 * timestamp, and for a timestamptz, timestamp[us] with UTC (convert.c).  A
 * dataset another writer made may have its timestamps in another unit or
 * zone, with which Lance compares a literal in the coarser unit and the
 * field's zone -- truncated to milliseconds, read as Moscow's time.
 */
static void
timestamp_fields(LtTable *t, const struct ArrowSchema *schema, LtVersionInfo *vi)
{
	for (int64 i = 0; i < schema->n_children; i++)
	{
		const struct ArrowSchema *f = schema->children[i];
		AttrNumber	attnum;

		if (f->name == NULL || f->format == NULL || strncmp(f->format, "tsu:", 4) != 0)
			continue;
		attnum = column_attnum(t, f->name);
		if (attnum == 0)
			continue;
		if (f->format[4] == '\0')
			vi->timestamps = bms_add_member(vi->timestamps, attnum);
		else if (strcmp(f->format + 4, "UTC") == 0 || strcmp(f->format + 4, "+00:00") == 0)
			vi->timestamptzs = bms_add_member(vi->timestamptzs, attnum);
	}
}

/*
 * A version's fragments, and its indices as the engine lists them, a segment
 * an entry: the segments of one index, which cover fragments apart, are
 * summed into one.  An engine that does not count an index's rows counts
 * its fragments, taken as the same share of the rows.  And the version's
 * timestamp fields.
 */
static LtVersionInfo *
load_version_info(LtTable *t, uint64 version)
{
	LeDataset  *ds = lt_open_dataset(t, version);
	LeDatasetInfo info;
	struct ArrowSchema schema;
	LeError		err = {0};
	char	   *json = NULL;
	int			rc;
	Jsonb	   *jb;
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken tok;
	LtVersionInfo *vi;
	LtIndexInfo *out;
	int			n = 0;
	int			cap = 4;

	memset(&info, 0, sizeof(info));
	memset(&schema, 0, sizeof(schema));
	rc = lt_api()->indices(ds, &json, &err);
	if (rc == LE_OK)
		rc = lt_api()->dataset_info(ds, &info, &err);
	if (rc == LE_OK)
		rc = lt_api()->dataset_schema(ds, &schema, &err);
	lt_api()->dataset_close(ds);
	vi = palloc0(sizeof(LtVersionInfo));
	if (rc == LE_OK)
		timestamp_fields(t, &schema, vi);
	if (schema.release)
		schema.release(&schema);
	if (rc != LE_OK)
		lt_raise(&err, "reading a lance table's indices");
	json = lt_take_string(json);
	out = palloc0(sizeof(LtIndexInfo) * cap);
	jb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json)));
	it = JsonbIteratorInit(&jb->root);
	while ((tok = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
	{
		JsonbContainer *obj;
		JsonbValue *name;
		JsonbValue *columns;
		JsonbValue *col;
		AttrNumber	attnum = 0;
		double		rows;
		int			i;

		if (tok != WJB_ELEM || v.type != jbvBinary)
			continue;
		obj = v.val.binary.data;
		name = json_member(obj, "name");
		columns = json_member(obj, "columns");
		if (name == NULL || name->type != jbvString || columns == NULL ||
			columns->type != jbvBinary || JsonContainerSize(columns->val.binary.data) != 1)
			continue;
		col = getIthJsonbValueFromContainer(columns->val.binary.data, 0);
		if (col == NULL || col->type != jbvString)
			continue;
		for (int a = 0; a < t->natts; a++)
		{
			if (t->columns[a] && strlen(t->columns[a]) == col->val.string.len &&
				memcmp(t->columns[a], col->val.string.val, col->val.string.len) == 0)
				attnum = a + 1;
		}
		rows = json_number(obj, "rows", -1);
		if (rows < 0)
			rows = info.num_fragments == 0 ? 0 :
				(double) info.num_rows * Min(json_number(obj, "fragments", 0) /
											 (double) info.num_fragments, 1.0);
		for (i = 0; i < n; i++)
		{
			if (strlen(out[i].name) == name->val.string.len &&
				memcmp(out[i].name, name->val.string.val, name->val.string.len) == 0)
				break;
		}
		if (i == n)
		{
			if (n == cap)
			{
				cap *= 2;
				out = repalloc(out, sizeof(LtIndexInfo) * cap);
			}
			out[n].name = pnstrdup(name->val.string.val, name->val.string.len);
			out[n].attnum = attnum;
			out[n].kind = index_kind(json_member(obj, "type"));
			out[n].rows = 0;
			n++;
		}
		out[i].rows = Min(out[i].rows + rows, (double) info.num_rows);
	}
	vi->version = version;
	vi->nfragments = info.num_fragments;
	vi->nindices = n;
	vi->indices = out;
	return vi;
}

static LtVersionInfo *
copy_version_info(const LtVersionInfo *src)
{
	LtVersionInfo *vi = palloc(sizeof(LtVersionInfo));

	*vi = *src;
	vi->indices = palloc(sizeof(LtIndexInfo) * Max(src->nindices, 1));
	for (int i = 0; i < src->nindices; i++)
	{
		vi->indices[i] = src->indices[i];
		vi->indices[i].name = pstrdup(src->indices[i].name);
	}
	vi->timestamps = bms_copy(src->timestamps);
	vi->timestamptzs = bms_copy(src->timestamptzs);
	return vi;
}

/*
 * A version of the table's dataset, for the planner: its fragments, indices
 * and timestamp fields, read from the engine once a version, since a
 * version's never change, and kept until the table's row is.
 */
LtVersionInfo *
lt_version_info(Relation rel, uint64 version)
{
	Oid			relid = RelationGetRelid(rel);
	LtTable    *t = lt_table_copy_by_relid(relid, false);
	TableEntry *e = hash_search(tables, &relid, HASH_FIND, NULL);
	LtVersionInfo *loaded;

	if (e && e->table && e->vinfo && e->vinfo->version == version)
		return copy_version_info(e->vinfo);
	loaded = load_version_info(t, version);

	/* an invalidation while the engine read it may have dropped the entry */
	e = hash_search(tables, &relid, HASH_FIND, NULL);
	if (e && e->table)
	{
		MemoryContext old;

		if (e->vinfo_cxt == NULL)
			e->vinfo_cxt = AllocSetContextCreate(e->cxt, "lance_tam version",
												 ALLOCSET_SMALL_SIZES);
		else
			MemoryContextReset(e->vinfo_cxt);
		old = MemoryContextSwitchTo(e->vinfo_cxt);
		e->vinfo = copy_version_info(loaded);
		MemoryContextSwitchTo(old);
	}
	return loaded;
}

/*
 * The version a snapshot reads.  A table that follows its dataset reads
 * Lance's latest version, the one latest when the statement first asked.
 */
uint64
lt_snapshot_version(Relation rel, LtTable *t, Snapshot snapshot)
{
	static Oid	follow_relid = InvalidOid;
	static TimestampTz follow_stmt = 0;
	static uint64 follow_version = 0;

	if (t->follow)
	{
		TimestampTz stmt = GetCurrentStatementStartTimestamp();
		LeError		err = {0};
		uint64		v;

		if (follow_relid == t->relid && follow_stmt == stmt && follow_version > 0)
			return follow_version;
		if (lt_api()->latest_version(t->uri, t->storage_json, &v, &err) != LE_OK)
			lt_raise(&err, "reading the latest version of a lance table");
		follow_relid = t->relid;
		follow_stmt = stmt;
		follow_version = v;
		return v;
	}
	return lt_catalog_version(t->relid, snapshot);
}

/* ------------------------------------------------------------------------
 * Changing the catalog
 * ------------------------------------------------------------------------
 */

static Datum
text_array_datum(List *strings)
{
	int			n = list_length(strings);
	Datum	   *elems = palloc(sizeof(Datum) * Max(n, 1));
	ListCell   *lc;
	int			i = 0;

	foreach(lc, strings)
	{
		char	   *s = lfirst(lc);

		elems[i++] = CStringGetTextDatum(s ? s : "");
	}
	return PointerGetDatum(construct_array_builtin(elems, n, TEXTOID));
}

static List *
options_to_strings(List *options)
{
	List	   *out = NIL;
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *d = lfirst(lc);

		out = lappend(out, d->arg ? psprintf("%s=%s", d->defname, defGetString(d)) : pstrdup(d->defname));
	}
	return out;
}

void
lt_catalog_insert(Oid relid, const char *uri, uint64 version, bool managed, bool writable,
				  bool follow, List *columns, List *options, const char *server)
{
	Relation	catrel = table_open(lt_catalog_relid(), RowExclusiveLock);
	Datum		values[Natts_tables];
	bool		nulls[Natts_tables];
	HeapTuple	tup;

	memset(nulls, false, sizeof(nulls));
	values[Anum_tables_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_tables_uri - 1] = CStringGetTextDatum(uri);
	values[Anum_tables_version - 1] = Int64GetDatum((int64) version);
	values[Anum_tables_managed - 1] = BoolGetDatum(managed);
	values[Anum_tables_writable - 1] = BoolGetDatum(writable);
	values[Anum_tables_follow - 1] = BoolGetDatum(follow);
	values[Anum_tables_columns - 1] = text_array_datum(columns);
	if (options != NIL)
		values[Anum_tables_options - 1] = text_array_datum(options_to_strings(options));
	else
		nulls[Anum_tables_options - 1] = true;
	if (server)
		values[Anum_tables_server - 1] = CStringGetTextDatum(server);
	else
		nulls[Anum_tables_server - 1] = true;

	tup = heap_form_tuple(RelationGetDescr(catrel), values, nulls);
	CatalogTupleInsert(catrel, tup);
	heap_freetuple(tup);
	table_close(catrel, RowExclusiveLock);
	CommandCounterIncrement();
	lt_table_invalidate(relid);
}

static void
catalog_update(Oid relid, int attnum, Datum value)
{
	Relation	catrel = table_open(lt_catalog_relid(), RowExclusiveLock);
	HeapTuple	tup = catalog_fetch(catrel, relid, SnapshotSelf);
	Datum		values[Natts_tables];
	bool		nulls[Natts_tables];
	bool		replace[Natts_tables];
	HeapTuple	newtup;

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "lance_tam: relation %u has no row in lance.tables", relid);
	memset(replace, false, sizeof(replace));
	memset(nulls, false, sizeof(nulls));
	values[attnum - 1] = value;
	replace[attnum - 1] = true;
	newtup = heap_modify_tuple(tup, RelationGetDescr(catrel), values, nulls, replace);
	CatalogTupleUpdate(catrel, &tup->t_self, newtup);
	heap_freetuple(newtup);
	heap_freetuple(tup);
	table_close(catrel, RowExclusiveLock);
	CommandCounterIncrement();
}

void
lt_catalog_set_version(Oid relid, uint64 version)
{
	catalog_update(relid, Anum_tables_version, Int64GetDatum((int64) version));
}

void
lt_catalog_set_columns(Oid relid, List *columns)
{
	catalog_update(relid, Anum_tables_columns, text_array_datum(columns));
	lt_table_invalidate(relid);
}

void
lt_catalog_delete(Oid relid)
{
	Relation	catrel = table_open(lt_catalog_relid(), RowExclusiveLock);
	HeapTuple	tup = catalog_fetch(catrel, relid, SnapshotSelf);

	if (HeapTupleIsValid(tup))
	{
		CatalogTupleDelete(catrel, &tup->t_self);
		heap_freetuple(tup);
	}
	table_close(catrel, RowExclusiveLock);
	CommandCounterIncrement();
	lt_table_invalidate(relid);
}

/*
 * The commit lock of a table: writers commit one after another, each on
 * the version the last committed, the lock held until the transaction ends.
 */
void
lt_lock_table_commit(Oid relid)
{
	LockDatabaseObject(lt_catalog_relid(), relid, 0, ExclusiveLock);
}

/* ------------------------------------------------------------------------
 * Row ids
 * ------------------------------------------------------------------------
 */

/*
 * The next row id: from a chunk of LT_ROWID_CHUNK ids this backend took
 * from lance.rowid_seq, whose every value is a chunk's first id.  Ids a
 * backend leaves unused are never given; ids are never reused.  A chunk
 * taken starts above rowid_floor, the highest id of a dataset this backend
 * has written: the chunks below it are passed over, nextval() after
 * nextval(), so that the sequence only moves forward and its users never
 * wait for each other -- nextval() holds RowExclusiveLock on it to the end
 * of its transaction.
 */
static uint64 rowid_next = 0;
static uint64 rowid_end = 0;
static uint64 rowid_floor = 0;

static void
take_chunk(void)
{
	Oid			nsp = get_namespace_oid("lance", false);
	Oid			seq = get_relname_relid("rowid_seq", nsp);
	int64		start;

	if (!OidIsValid(seq))
		elog(ERROR, "lance_tam: lance.rowid_seq is missing");
	do
	{
		CHECK_FOR_INTERRUPTS();
		start = nextval_internal(seq, false);
		if (start < (int64) LT_ROWID_START || (uint64) start + LT_ROWID_CHUNK > LT_ROWID_LIMIT)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("lance tables have no row ids left")));
	} while ((uint64) start <= rowid_floor);
	rowid_next = (uint64) start;
	rowid_end = rowid_next + LT_ROWID_CHUNK;
}

uint64
lt_next_rowid(void)
{
	if (rowid_next >= rowid_end)
		take_chunk();
	return rowid_next++;
}

/*
 * Before a transaction's first write of a dataset: the ids given from now
 * on are none the dataset holds.  A dataset can hold ids the sequence has
 * not given -- it outlived the sequence (the extension made again), another
 * server wrote it -- so the backend's chunk is left when the dataset holds
 * one of the ids it has left, and the chunks taken after it start above the
 * dataset's highest id.  The ids a chunk has left are this backend's alone:
 * another writer's rows above them, or the dataset's own rows below them,
 * leave the chunk in use.
 */
void
lt_rowids_for(LeDataset *ds)
{
	uint64		max_row_id;
	bool		held;
	LeError		err = {0};

	if (lt_api()->row_ids_held(ds, rowid_next, rowid_end, &max_row_id, &held, &err) != LE_OK)
		lt_raise(&err, "reading the dataset's row ids");
	if (max_row_id < LT_ROWID_START)
		return;
	if ((max_row_id / LT_ROWID_CHUNK + 2) * LT_ROWID_CHUNK > LT_ROWID_LIMIT)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("the dataset's row ids leave no room for new rows")));
	rowid_floor = Max(rowid_floor, max_row_id);
	if (held)
		rowid_next = rowid_end = 0;
}
