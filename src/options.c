/*-------------------------------------------------------------------------
 *
 * options.c
 *	  A lance table's options, and the DDL the extension watches
 *	  (docs/design.md §2.4).
 *
 *	  Vanilla PostgreSQL 19 has no table-AM option parser: CREATE TABLE
 *	  validates WITH (...) as heap's options, and heap's parser refuses a
 *	  name it does not know (tablecmds.c, reloptions.c).  So the
 *	  ProcessUtility hook takes a lance table's options out of CREATE TABLE
 *	  and CREATE TABLE AS, lets PostgreSQL make the relation, and hands the
 *	  options to the access method's relation_set_new_filelocator(), which
 *	  makes or attaches the dataset and writes the table's catalog row.  A
 *	  CREATE TABLE of no columns over a dataset that exists takes its
 *	  columns from the dataset.
 *
 *	  The hook refuses what would leave the dataset behind PostgreSQL's
 *	  relation: SET LOGGED or UNLOGGED, SET ACCESS METHOD to or from lance,
 *	  and CREATE INDEX CONCURRENTLY; an ALTER TABLE that rewrites the table
 *	  is refused when the rewrite begins (tableam.c).  The object access
 *	  hook sees DROP TABLE -- the catalog row goes with the transaction, a
 *	  managed dataset when it commits -- the columns ALTER TABLE ... ADD
 *	  COLUMN adds, which become the dataset's when the statement ends, and
 *	  ALTER COLUMN ... TYPE, which must leave the dataset's column as it is.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "lance_tam.h"

static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static object_access_hook_type prev_object_access = NULL;

/* The options of the CREATE being run, in TopTransactionContext */
static List *pending_options = NIL;
static int	utility_depth = 0;

/*
 * The columns ALTER TABLE ... ADD COLUMN gave lance tables and DROP COLUMN
 * took, made the datasets' when the statement ends: a relation and an
 * attribute number each, in TopTransactionContext.
 */
static List *added_relids = NIL;
static List *added_attnums = NIL;
static List *dropped_relids = NIL;
static List *dropped_attnums = NIL;

static void add_noted_columns(void);

List *
lt_pending_create_options(void)
{
	List	   *o = pending_options;

	pending_options = NIL;
	return o;
}

static bool
am_is_lance(const char *am)
{
	if (am == NULL)
		am = default_table_access_method;
	return am != NULL && strcmp(am, "lance") == 0;
}

static const char *const secret_words[] = {"secret", "password", "token", "credential", NULL};

/* Options a table may not carry: credentials go in a user mapping */
static void
check_option(DefElem *d)
{
	char	   *lower = asc_tolower(d->defname, strlen(d->defname));

	for (int i = 0; secret_words[i]; i++)
		if (strstr(lower, secret_words[i]) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" looks like a credential, which a lance table's options may not hold",
							d->defname),
					 errdetail("A table's options are visible to every user."),
					 errhint("Put credentials in a user mapping: CREATE SERVER ... FOREIGN DATA WRAPPER lance_storage, then CREATE USER MAPPING ... OPTIONS (...), and name the server with WITH (server = '...').")));
	for (const char *p = d->defname; *p; p++)
		if (!(isalnum((unsigned char) *p) || *p == '_' || *p == '.' || *p == '-'))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid option name \"%s\"", d->defname)));
}

/*
 * PostgreSQL's own storage parameters of a table -- autovacuum's and
 * VACUUM's, parallel_workers, fillfactor, the toast table's -- which stay
 * PostgreSQL's to validate and keep: autovacuum_enabled and
 * parallel_workers mean for a lance table what they mean for heap.
 */
static bool
is_pg_reloption(DefElem *d)
{
	static const char *const prefixes[] = {
		"autovacuum_", "vacuum_", "log_autovacuum_", "log_autoanalyze_", NULL
	};
	static const char *const names[] = {
		"fillfactor", "toast_tuple_target", "parallel_workers", "user_catalog_table",
		"oids", NULL
	};

	if (d->defnamespace != NULL)
		return true;
	for (int i = 0; prefixes[i]; i++)
		if (pg_strncasecmp(d->defname, prefixes[i], strlen(prefixes[i])) == 0)
			return true;
	for (int i = 0; names[i]; i++)
		if (pg_strcasecmp(d->defname, names[i]) == 0)
			return true;
	return false;
}

/* The lance table's options, out of a WITH (...): PostgreSQL's stay */
static List *
take_options(List **options)
{
	List	   *taken = NIL;
	List	   *kept = NIL;
	ListCell   *lc;
	MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

	foreach(lc, *options)
	{
		DefElem    *d = lfirst(lc);
		DefElem    *copy;

		if (is_pg_reloption(d))
		{
			kept = lappend(kept, d);
			continue;
		}
		check_option(d);
		copy = makeDefElem(pstrdup(d->defname),
						   d->arg ? (Node *) makeString(pstrdup(defGetString(d))) : NULL, -1);
		if (strcmp(copy->defname, "max_rows_per_file") == 0 ||
			strcmp(copy->defname, "max_rows_per_group") == 0 ||
			strcmp(copy->defname, "buffer_rows") == 0)
			(void) lt_option_int(list_make1(copy), copy->defname, 0);
		if (strcmp(copy->defname, "compression") == 0 ||
			strcmp(copy->defname, "compression_level") == 0 ||
			strcmp(copy->defname, "column_encoding") == 0)
			lt_check_encoding_option(copy);
		taken = lappend(taken, copy);
	}
	MemoryContextSwitchTo(old);
	*options = kept;
	return taken;
}

/*
 * CREATE TABLE t () USING lance WITH (uri = ...) over a dataset that exists:
 * the dataset's columns, as column definitions.
 */
static void
columns_from_dataset(CreateStmt *stmt, List *options)
{
	char	   *uri = lt_option_string(options, "uri");
	char	   *server = lt_option_string(options, "server");
	char	   *json;
	const LeApi *api;
	LeDataset  *ds = NULL;
	LeError		err = {0};
	struct ArrowSchema schema;
	int			rc;

	if (server == NULL && lt_default_server && lt_default_server[0])
		server = lt_default_server;

	if (uri == NULL || stmt->tableElts != NIL || stmt->ofTypename || stmt->partbound ||
		stmt->inhRelations != NIL)
		return;
	json = lt_storage_json(options, server, GetUserId());
	api = lt_api();
	rc = api->dataset_open(uri, json, -1, &ds, &err);
	if (rc == LE_ERR_NOT_FOUND)
	{
		lt_take_string(err.message);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("no Lance dataset at \"%s\" to take the table's columns from", uri),
				 errhint("Name the columns to make a new dataset there.")));
	}
	if (rc != LE_OK)
		lt_raise(&err, "opening the dataset to take its columns");
	memset(&schema, 0, sizeof(schema));
	if (api->dataset_schema(ds, &schema, &err) != LE_OK)
	{
		api->dataset_close(ds);
		lt_raise(&err, "reading the dataset's schema");
	}
	api->dataset_close(ds);
	PG_TRY();
	{
		for (int64 i = 0; i < schema.n_children; i++)
		{
			struct ArrowSchema *f = schema.children[i];
			char	   *comment;
			char	   *typname = lt_arrow_to_pg_type(f, &comment);
			ColumnDef  *col;

			if (typname == NULL)
			{
				ereport(NOTICE,
						(errmsg("the dataset's column \"%s\" is left out: no PostgreSQL type maps its %s",
								f->name, comment ? comment : "type")));
				continue;
			}
			col = makeColumnDef(f->name, InvalidOid, -1, InvalidOid);
			col->typeName = typeStringToTypeName(typname, NULL);
			col->is_not_null = (f->flags & ARROW_FLAG_NULLABLE) == 0;
			stmt->tableElts = lappend(stmt->tableElts, col);
		}
	}
	PG_FINALLY();
	{
		if (schema.release)
			schema.release(&schema);
	}
	PG_END_TRY();
	if (stmt->tableElts == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("no column of the dataset at \"%s\" maps to a PostgreSQL type", uri)));
}

static bool
relation_is_lance(RangeVar *rv, bool missing_ok)
{
	Oid			relid = RangeVarGetRelid(rv, NoLock, true);

	if (!OidIsValid(relid))
		return false;
	return lt_is_lance_relid(relid);
}

static void
check_alter_table(AlterTableStmt *stmt)
{
	ListCell   *lc;
	bool		lance;

	if (stmt->objtype != OBJECT_TABLE && stmt->objtype != OBJECT_MATVIEW)
		return;
	lance = relation_is_lance(stmt->relation, true);
	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = lfirst(lc);

		if (cmd->subtype == AT_SetAccessMethod)
		{
			if (lance || (cmd->name && strcmp(cmd->name, "lance") == 0))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot change the access method of a table to or from lance"),
						 errhint("Create a new table and copy the rows: CREATE TABLE ... USING lance; INSERT INTO ... SELECT ...")));
			continue;
		}
		if (!lance)
			continue;
		switch (cmd->subtype)
		{
			case AT_SetRelOptions:
			case AT_ResetRelOptions:
			case AT_ReplaceRelOptions:
				{
					ListCell   *lc2;

					/* PostgreSQL's own storage parameters, as for heap */
					foreach(lc2, (List *) cmd->def)
					{
						DefElem    *d = lfirst(lc2);

						if (!is_pg_reloption(d))
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("option \"%s\" of a lance table cannot be changed by ALTER TABLE",
											d->defname),
									 errdetail("A lance table's own options are set when the table is created.")));
					}
				}
				break;
			case AT_SetLogged:
			case AT_SetUnLogged:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("this ALTER TABLE is not supported on lance tables"),
						 errdetail("It would change the table's columns or rewrite it apart from its Lance dataset.")));
				break;
			default:
				break;
		}
	}
}

/* Whether a relation has an index of lance_index */
static bool
has_lance_index(Oid relid)
{
	Relation	rel = table_open(relid, AccessShareLock);
	List	   *indexes = RelationGetIndexList(rel);
	ListCell   *lc;
	bool		found = false;

	foreach(lc, indexes)
	{
		HeapTuple	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(lfirst_oid(lc)));

		if (HeapTupleIsValid(tup))
		{
			if (lt_is_lance_index_am(((Form_pg_class) GETSTRUCT(tup))->relam))
				found = true;
			ReleaseSysCache(tup);
		}
	}
	list_free(indexes);
	table_close(rel, AccessShareLock);
	return found;
}

/*
 * REINDEX CONCURRENTLY builds a copy of each index under another name and
 * swaps the names: a lance_index, which is a Lance index by its name,
 * cannot take part.
 */
static void
check_reindex(ReindexStmt *stmt)
{
	ListCell   *lc;
	bool		concurrently = false;
	Oid			relid;

	foreach(lc, stmt->params)
	{
		DefElem    *opt = lfirst(lc);

		if (strcmp(opt->defname, "concurrently") == 0)
			concurrently = defGetBoolean(opt);
	}
	if (!concurrently || stmt->relation == NULL)
		return;
	relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	if (!OidIsValid(relid))
		return;
	if ((stmt->kind == REINDEX_OBJECT_INDEX && get_rel_relkind(relid) == RELKIND_INDEX &&
		 lt_is_lance_index_am(get_rel_relam(relid))) ||
		(stmt->kind == REINDEX_OBJECT_TABLE && lt_is_lance_relid(relid) && has_lance_index(relid)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a lance_index cannot be rebuilt concurrently"),
				 errhint("REINDEX it without CONCURRENTLY.")));
}

static void
lt_ProcessUtility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;
	List	   *saved_pending = pending_options;

	if (IsA(parsetree, CreateStmt) &&
		am_is_lance(((CreateStmt *) parsetree)->accessMethod) &&
		((CreateStmt *) parsetree)->partspec == NULL)
	{
		CreateStmt *stmt;

		if (readOnlyTree)
		{
			pstmt = copyObject(pstmt);
			parsetree = pstmt->utilityStmt;
			readOnlyTree = false;
		}
		stmt = (CreateStmt *) parsetree;
		pending_options = take_options(&stmt->options);
		columns_from_dataset(stmt, pending_options);
	}
	else if (IsA(parsetree, CreateStmt) &&
			 am_is_lance(((CreateStmt *) parsetree)->accessMethod))
	{
		ListCell   *lc;

		/* a partitioned table: no storage of its own, its partitions lance */
		foreach(lc, ((CreateStmt *) parsetree)->options)
		{
			if (!is_pg_reloption(lfirst(lc)))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("a partitioned lance table takes no options"),
						 errhint("Give each partition its options.")));
		}
	}
	else if (IsA(parsetree, CreateTableAsStmt) &&
			 ((CreateTableAsStmt *) parsetree)->objtype == OBJECT_TABLE &&
			 am_is_lance(((CreateTableAsStmt *) parsetree)->into->accessMethod))
	{
		CreateTableAsStmt *stmt;

		if (readOnlyTree)
		{
			pstmt = copyObject(pstmt);
			parsetree = pstmt->utilityStmt;
			readOnlyTree = false;
		}
		stmt = (CreateTableAsStmt *) parsetree;
		pending_options = take_options(&stmt->into->options);
	}
	else if (IsA(parsetree, CreateTableAsStmt) &&
			 ((CreateTableAsStmt *) parsetree)->objtype == OBJECT_MATVIEW &&
			 am_is_lance(((CreateTableAsStmt *) parsetree)->into->accessMethod))
	{
		/*
		 * REFRESH rewrites a materialized view, which a lance table cannot
		 * be: refused, whether it names lance or lance is the default.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialized views cannot use the lance access method"),
				 errhint("Create it USING heap.")));
	}
	else if (IsA(parsetree, AlterTableStmt))
		check_alter_table((AlterTableStmt *) parsetree);
	else if (IsA(parsetree, IndexStmt))
	{
		IndexStmt  *stmt = (IndexStmt *) parsetree;

		/*
		 * A concurrent build of PostgreSQL's own index is validated by the
		 * table's index_validate_scan(); a lance_index is the dataset's,
		 * built when its transaction commits, which a build in several
		 * transactions does not fit.
		 */
		if (stmt->concurrent && stmt->accessMethod &&
			strcmp(stmt->accessMethod, "lance_index") == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("a lance_index cannot be built concurrently")));
	}
	else if (IsA(parsetree, ReindexStmt))
		check_reindex((ReindexStmt *) parsetree);

	utility_depth++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
								queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
									queryEnv, dest, qc);
		add_noted_columns();
	}
	PG_FINALLY();
	{
		utility_depth--;
		pending_options = utility_depth > 0 ? saved_pending : NIL;
		if (utility_depth == 0)
		{
			added_relids = NIL;
			added_attnums = NIL;
			dropped_relids = NIL;
			dropped_attnums = NIL;
		}
	}
	PG_END_TRY();
}

/* The first noted table's attributes, taken off the lists */
static Oid
take_noted(List **relids, List **attnums, List **taken)
{
	Oid			relid = linitial_oid(*relids);
	List	   *rest_relids = NIL;
	List	   *rest_attnums = NIL;
	ListCell   *lr;
	ListCell   *la;
	MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

	*taken = NIL;
	forboth(lr, *relids, la, *attnums)
	{
		if (lfirst_oid(lr) == relid)
			*taken = list_append_unique_int(*taken, lfirst_int(la));
		else
		{
			rest_relids = lappend_oid(rest_relids, lfirst_oid(lr));
			rest_attnums = lappend_int(rest_attnums, lfirst_int(la));
		}
	}
	*relids = rest_relids;
	*attnums = rest_attnums;
	MemoryContextSwitchTo(old);
	return relid;
}

/*
 * The columns ALTER TABLE dropped and added, a table at a time: the
 * drops first, so that a name given again is free.
 */
static void
add_noted_columns(void)
{
	List	   *attnums;
	Oid			relid;

	while (dropped_relids != NIL)
	{
		relid = take_noted(&dropped_relids, &dropped_attnums, &attnums);
		if (get_rel_relkind(relid) == RELKIND_RELATION && lt_is_lance_relid(relid))
			lt_xact_drop_columns(relid, attnums);
	}
	while (added_relids != NIL)
	{
		relid = take_noted(&added_relids, &added_attnums, &attnums);
		if (get_rel_relkind(relid) == RELKIND_RELATION && lt_is_lance_relid(relid))
			lt_xact_add_columns(relid, attnums);
	}
}

/*
 * ALTER COLUMN ... TYPE that PostgreSQL makes in its catalog alone --
 * varchar(10) to varchar(20), to a domain over the type -- keeps the
 * dataset's column, which must hold the same Arrow values; one that
 * rewrites the table is refused when the rewrite begins.  At the hook the
 * relation's descriptor still has the attribute as it was, and the
 * catalog, read as the command's own changes, as it is.
 */
static void
check_type_change(Oid relid, AttrNumber attnum)
{
	Relation	rel = relation_open(relid, NoLock);
	Form_pg_attribute was;
	Relation	attrel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;

	if (attnum > RelationGetDescr(rel)->natts)
	{
		relation_close(rel, NoLock);
		return;
	}
	was = TupleDescAttr(RelationGetDescr(rel), attnum - 1);
	attrel = table_open(AttributeRelationId, AccessShareLock);
	ScanKeyInit(&key[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));
	ScanKeyInit(&key[1], Anum_pg_attribute_attnum, BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(attnum));
	scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true, SnapshotSelf, 2, key);
	tup = systable_getnext(scan);
	if (HeapTupleIsValid(tup))
	{
		Form_pg_attribute now = (Form_pg_attribute) GETSTRUCT(tup);

		if (!was->attisdropped && !now->attisdropped &&
			(now->atttypid != was->atttypid || now->atttypmod != was->atttypmod) &&
			!lt_attribute_maps_alike(was, now))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot change the type of column \"%s\" of lance table \"%s\" from %s to %s",
							NameStr(was->attname), RelationGetRelationName(rel),
							format_type_with_typemod(was->atttypid, was->atttypmod),
							format_type_with_typemod(now->atttypid, now->atttypmod)),
					 errdetail("The dataset's column would hold values of another Arrow type.")));
	}
	systable_endscan(scan);
	table_close(attrel, AccessShareLock);
	relation_close(rel, NoLock);
}

/* DROP INDEX of a lance_index: the dataset's index, dropped at commit */
static void
drop_lance_index(Oid indexoid)
{
	HeapTuple	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(indexoid));
	Form_pg_class form;
	Oid			heapoid;
	Relation	heap;
	LtXactTable *xt;
	LeError		err = {0};
	char	   *name;

	if (!HeapTupleIsValid(tup))
		return;
	form = (Form_pg_class) GETSTRUCT(tup);
	if (form->relkind != RELKIND_INDEX || !lt_is_lance_index_am(form->relam))
	{
		ReleaseSysCache(tup);
		return;
	}
	name = pstrdup(NameStr(form->relname));
	ReleaseSysCache(tup);
	heapoid = IndexGetRelation(indexoid, true);
	if (!OidIsValid(heapoid) || !lt_is_lance_relid(heapoid))
		return;
	heap = table_open(heapoid, NoLock);
	PG_TRY();
	{
		xt = lt_xact_table(heap, true);
		lt_xact_write(heap, xt);
		if (lt_api()->write_drop_index(xt->write, name, GetCurrentSubTransactionId(), &err) != LE_OK)
			lt_raise(&err, "dropping a Lance index");
	}
	PG_FINALLY();
	{
		table_close(heap, NoLock);
	}
	PG_END_TRY();
}

/* DROP TABLE of a lance table: its row now, its dataset at commit */
static void
lt_object_access(ObjectAccessType access, Oid classId, Oid objectId, int subId, void *arg)
{
	if (prev_object_access)
		prev_object_access(access, classId, objectId, subId, arg);

	if (access == OAT_POST_CREATE && classId == RelationRelationId && subId > 0 &&
		OidIsValid(get_namespace_oid("lance", true)) && lt_is_lance_relid(objectId))
	{
		/* ALTER TABLE ... ADD COLUMN: the dataset's column at the end */
		MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

		added_relids = lappend_oid(added_relids, objectId);
		added_attnums = lappend_int(added_attnums, subId);
		MemoryContextSwitchTo(old);
	}
	else if (access == OAT_POST_ALTER && classId == RelationRelationId && subId > 0 &&
			 OidIsValid(get_namespace_oid("lance", true)) && lt_is_lance_relid(objectId))
		check_type_change(objectId, (AttrNumber) subId);
	else if (access == OAT_DROP && classId == RelationRelationId && subId > 0 &&
			 OidIsValid(get_namespace_oid("lance", true)) && lt_is_lance_relid(objectId))
	{
		/* ALTER TABLE ... DROP COLUMN: the dataset's column too, at the end */
		MemoryContext old = MemoryContextSwitchTo(TopTransactionContext);

		dropped_relids = lappend_oid(dropped_relids, objectId);
		dropped_attnums = lappend_int(dropped_attnums, subId);
		MemoryContextSwitchTo(old);
	}
	else if (access == OAT_DROP && classId == RelationRelationId && subId == 0 &&
			 OidIsValid(get_namespace_oid("lance", true)) &&
			 get_rel_relkind(objectId) == RELKIND_INDEX)
		drop_lance_index(objectId);
	else if (access == OAT_DROP && classId == RelationRelationId && subId == 0 &&
			 OidIsValid(get_namespace_oid("lance", true)) &&
			 OidIsValid(get_relname_relid("tables", get_namespace_oid("lance", true))) &&
			 lt_is_lance_relid(objectId))
	{
		LtTable    *t = lt_table_by_relid(objectId, true);

		if (t != NULL)
		{
			lt_xact_note_dropped(objectId, t->uri, t->storage_json, t->managed);
			lt_catalog_delete(objectId);
		}
	}
}

void
lt_options_init(void)
{
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = lt_ProcessUtility;
	prev_object_access = object_access_hook;
	object_access_hook = lt_object_access;
}
