/*-------------------------------------------------------------------------
 *
 * functions.c
 *	  The extension's SQL functions (docs/design.md §2.10): what a lance
 *	  table's dataset holds, and its maintenance.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "lance_tam.h"

#define pg_class_ownercheck_compat(relid) \
	object_ownercheck(RelationRelationId, (relid), GetUserId())

static Relation
open_lance(Oid relid, LOCKMODE mode)
{
	Relation	rel = table_open(relid, mode);

	if (!lt_is_lance_rel(rel))
	{
		table_close(rel, mode);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a lance table", get_rel_name(relid))));
	}
	return rel;
}

static Datum
jsonb_datum(const char *json)
{
	return DirectFunctionCall1(jsonb_in, CStringGetDatum(json));
}

PG_FUNCTION_INFO_V1(lance_table_info);
Datum
lance_table_info(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel = open_lance(relid, AccessShareLock);
	LtTable    *t = lt_table(rel);
	uint64		version = lt_snapshot_version(rel, t, GetActiveSnapshot());
	LeDataset  *ds = lt_open_dataset(t, version);
	LeDatasetInfo info;
	LeError		err = {0};
	StringInfoData buf;
	uint64		latest = 0;
	int			rc;

	memset(&info, 0, sizeof(info));
	rc = lt_api()->dataset_info(ds, &info, &err);
	lt_api()->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "reading a lance table's dataset");
	if (lt_api()->latest_version(t->uri, t->storage_json, &latest, &err) != LE_OK)
		lt_raise(&err, "reading a lance table's latest version");

	initStringInfo(&buf);
	appendStringInfoString(&buf, "{\"uri\":");
	escape_json(&buf, t->uri);
	appendStringInfo(&buf,
					 ",\"version\":" UINT64_FORMAT ",\"latest_version\":" UINT64_FORMAT
					 ",\"managed\":%s,\"writable\":%s,\"follow\":%s,\"rows\":" UINT64_FORMAT
					 ",\"deleted_rows\":" UINT64_FORMAT ",\"fragments\":" UINT64_FORMAT
					 ",\"data_bytes\":" UINT64_FORMAT ",\"stable_row_ids\":%s",
					 version, latest, t->managed ? "true" : "false",
					 t->writable ? "true" : "false", t->follow ? "true" : "false",
					 info.num_rows, info.num_deleted_rows, info.num_fragments, info.data_bytes,
					 info.stable_row_ids ? "true" : "false");
	if (t->server)
	{
		appendStringInfoString(&buf, ",\"server\":");
		escape_json(&buf, t->server);
	}
	appendStringInfoChar(&buf, '}');
	table_close(rel, AccessShareLock);
	PG_RETURN_DATUM(jsonb_datum(buf.data));
}

/* An Arrow schema's fields as a JSON array: name, format, metadata, children */
static void
append_fields(StringInfo buf, const struct ArrowSchema *s)
{
	appendStringInfoChar(buf, '[');
	for (int64 i = 0; i < s->n_children; i++)
	{
		const struct ArrowSchema *f = s->children[i];

		if (i > 0)
			appendStringInfoChar(buf, ',');
		appendStringInfoString(buf, "{\"name\":");
		escape_json(buf, f->name ? f->name : "");
		appendStringInfoString(buf, ",\"format\":");
		escape_json(buf, f->format ? f->format : "");
		appendStringInfo(buf, ",\"nullable\":%s",
						 (f->flags & ARROW_FLAG_NULLABLE) ? "true" : "false");
		if (f->metadata)
		{
			appendStringInfoString(buf, ",\"metadata\":");
			lt_append_arrow_metadata(buf, f->metadata);
		}
		if (f->n_children > 0)
		{
			appendStringInfoString(buf, ",\"children\":");
			append_fields(buf, f);
		}
		appendStringInfoChar(buf, '}');
	}
	appendStringInfoChar(buf, ']');
}

PG_FUNCTION_INFO_V1(lance_schema);
Datum
lance_schema(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel = open_lance(relid, AccessShareLock);
	LtTable    *t = lt_table(rel);
	uint64		version = lt_snapshot_version(rel, t, GetActiveSnapshot());
	LeDataset  *ds = lt_open_dataset(t, version);
	struct ArrowSchema schema;
	LeError		err = {0};
	StringInfoData buf;
	int			rc;

	memset(&schema, 0, sizeof(schema));
	rc = lt_api()->dataset_schema(ds, &schema, &err);
	lt_api()->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "reading a lance table's schema");
	initStringInfo(&buf);
	PG_TRY();
	{
		append_fields(&buf, &schema);
	}
	PG_FINALLY();
	{
		if (schema.release)
			schema.release(&schema);
	}
	PG_END_TRY();
	table_close(rel, AccessShareLock);
	PG_RETURN_DATUM(jsonb_datum(buf.data));
}

PG_FUNCTION_INFO_V1(lance_versions);
Datum
lance_versions(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel = open_lance(relid, AccessShareLock);
	LtTable    *t = lt_table(rel);
	LeError		err = {0};
	char	   *json = NULL;

	if (lt_api()->versions(t->uri, t->storage_json, 0, &json, &err) != LE_OK)
		lt_raise(&err, "listing a lance table's versions");
	json = lt_take_string(json);
	table_close(rel, AccessShareLock);
	PG_RETURN_DATUM(jsonb_datum(json));
}

static Datum
dataset_json(Oid relid, int which)
{
	Relation	rel = open_lance(relid, AccessShareLock);
	LtTable    *t = lt_table(rel);
	uint64		version = lt_snapshot_version(rel, t, GetActiveSnapshot());
	LeDataset  *ds = lt_open_dataset(t, version);
	LeError		err = {0};
	char	   *json = NULL;
	int			rc;

	if (which == 0)
		rc = lt_api()->indices(ds, &json, &err);
	else
		rc = lt_api()->fragments(ds, &json, &err);
	lt_api()->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "reading a lance table's dataset");
	json = lt_take_string(json);
	table_close(rel, AccessShareLock);
	return jsonb_datum(json);
}

PG_FUNCTION_INFO_V1(lance_indices);
Datum
lance_indices(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(dataset_json(PG_GETARG_OID(0), 0));
}

PG_FUNCTION_INFO_V1(lance_fragments);
Datum
lance_fragments(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(dataset_json(PG_GETARG_OID(0), 1));
}

/*
 * lance.optimize(rel, options): the dataset's fragments compacted and its
 * indices brought up to its new rows, under the table's commit lock, and
 * the catalog set to the version that leaves.  Options as the engine takes
 * them: target_rows_per_fragment, materialize_deletions,
 * materialize_deletions_threshold, compact, indices.
 */
PG_FUNCTION_INFO_V1(lance_optimize);
Datum
lance_optimize(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Datum		bounds = DirectFunctionCall1(jsonb_in, CStringGetDatum(lt_compaction_bounds(0)));
	char	   *options;
	Relation	rel = open_lance(relid, ShareUpdateExclusiveLock);
	LtTable    *t = lt_table(rel);
	char	   *props;
	uint64		version;
	uint64		new_version = 0;
	LeDataset  *ds;
	LeError		err = {0};
	char	   *result = NULL;
	int			rc;

	if (!pg_class_ownercheck_compat(relid))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, RelationGetRelationName(rel));
	if (!t->writable)
		ereport(ERROR,
				(errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
				 errmsg("lance table \"%s\" is read-only", RelationGetRelationName(rel))));
	PreventInTransactionBlock(true, "lance.optimize()");
	/* VACUUM's bounds of compaction, unless the options say otherwise */
	options = DatumGetCString(DirectFunctionCall1(jsonb_out, PG_ARGISNULL(1) ? bounds :
												  DirectFunctionCall2(jsonb_concat, bounds,
																	  PG_GETARG_DATUM(1))));
	lt_lock_table_commit(relid);
	props = lt_xact_properties(relid);
	version = lt_commit_base(relid, t->uri, t->storage_json,
							 lt_catalog_version(relid, NULL), props);
	ds = lt_open_dataset(t, version);
	rc = lt_api()->optimize(ds, options, props, &new_version, &result, &err);
	lt_api()->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "optimizing a lance table");
	if (new_version != lt_catalog_version(relid, NULL))
		lt_catalog_set_version(relid, Max(new_version, version));
	table_close(rel, NoLock);
	PG_RETURN_DATUM(jsonb_datum(lt_take_string(result)));
}

PG_FUNCTION_INFO_V1(lance_cleanup);
Datum
lance_cleanup(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Interval   *older = PG_GETARG_INTERVAL_P(1);
	Relation	rel = open_lance(relid, ShareUpdateExclusiveLock);
	LtTable    *t = lt_table(rel);
	LeDataset  *ds;
	LeError		err = {0};
	char	   *result = NULL;
	int64		seconds;
	int			rc;

	if (!pg_class_ownercheck_compat(relid))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, RelationGetRelationName(rel));
	seconds = older->time / USECS_PER_SEC + (int64) older->day * SECS_PER_DAY +
		(int64) older->month * 30 * SECS_PER_DAY;
	ds = lt_open_dataset(t, lt_catalog_version(relid, NULL));
	rc = lt_api()->cleanup(ds, seconds, &result, &err);
	lt_api()->dataset_close(ds);
	if (rc != LE_OK)
		lt_raise(&err, "removing a lance table's old versions");
	table_close(rel, NoLock);
	PG_RETURN_DATUM(jsonb_datum(lt_take_string(result)));
}

/*
 * lance.refresh(rel): the table becomes its dataset's latest version, with
 * what other writers of the dataset committed since its own last commit.
 * Its PostgreSQL indexes do not hold those writers' rows: REINDEX them.
 */
PG_FUNCTION_INFO_V1(lance_refresh);
Datum
lance_refresh(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel = open_lance(relid, ShareRowExclusiveLock);
	LtTable    *t = lt_table(rel);
	uint64		catalog_version;
	uint64		version;
	uint64		latest = 0;
	LeError		err = {0};

	if (!pg_class_ownercheck_compat(relid))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, RelationGetRelationName(rel));
	if (t->follow)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lance table \"%s\" follows its dataset already", RelationGetRelationName(rel))));
	lt_lock_table_commit(relid);
	catalog_version = lt_catalog_version(relid, NULL);
	version = lt_commit_base(relid, t->uri, t->storage_json, catalog_version,
							 lt_xact_properties(relid));
	if (lt_api()->latest_version(t->uri, t->storage_json, &latest, &err) != LE_OK)
		lt_raise(&err, "reading a lance table's latest version");
	if (latest > version)
		version = latest;
	if (version != catalog_version)
	{
		lt_catalog_set_version(relid, version);
		if (rel->rd_rel->relhasindex)
			ereport(NOTICE,
					(errmsg("the indexes of \"%s\" do not hold the rows other writers added",
							RelationGetRelationName(rel)),
					 errhint("REINDEX TABLE %s;", quote_identifier(RelationGetRelationName(rel)))));
	}
	table_close(rel, NoLock);
	PG_RETURN_INT64((int64) version);
}

/* lance.explain(rel, filter): Lance's plan of a scan of the table */
PG_FUNCTION_INFO_V1(lance_explain);
Datum
lance_explain(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel = open_lance(relid, AccessShareLock);
	LtReadSpec	spec;
	char	   *plan;

	memset(&spec, 0, sizeof(spec));
	spec.limit = -1;
	spec.filter = PG_ARGISNULL(1) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(1));
	plan = lt_reader_explain(rel, GetActiveSnapshot(), &spec, PG_NARGS() > 2 && PG_GETARG_BOOL(2));
	table_close(rel, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(plan));
}

PG_FUNCTION_INFO_V1(lance_engine_version);
Datum
lance_engine_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(lt_api()->library_version));
}

/*
 * The foreign-data wrapper lance_storage's validator: a server's options
 * and a user mapping's are storage options, a name each of lowercase
 * letters, digits and _ . -
 */
PG_FUNCTION_INFO_V1(lance_storage_validator);
Datum
lance_storage_validator(PG_FUNCTION_ARGS)
{
	List	   *options = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *d = lfirst(lc);

		for (const char *p = d->defname; *p; p++)
			if (!(isalnum((unsigned char) *p) || *p == '_' || *p == '.' || *p == '-'))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid storage option name \"%s\"", d->defname)));
		if (catalog != ForeignServerRelationId && catalog != UserMappingRelationId)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("lance_storage takes options on servers and user mappings only")));
	}
	PG_RETURN_VOID();
}
