/*-------------------------------------------------------------------------
 *
 * indexam.c
 *	  lance_index (docs/design.md §2.9): Lance's own indices of a lance
 *	  table's dataset as PostgreSQL indexes.
 *
 *		CREATE INDEX ON items USING lance_index (embedding)
 *			WITH (type = 'ivf_hnsw_sq', metric = 'cosine');
 *		CREATE INDEX ON docs USING lance_index (body) WITH (type = 'inverted');
 *
 *	  The PostgreSQL index is the handle of the dataset's index: CREATE
 *	  INDEX and REINDEX build Lance's (vector: IVF_PQ, IVF_FLAT, IVF_SQ,
 *	  IVF_HNSW_SQ, IVF_HNSW_PQ, IVF_HNSW_FLAT, IVF_RQ; scalar: BTREE,
 *	  BITMAP, LABEL_LIST, INVERTED, NGRAM, ZONEMAP, BLOOMFILTER), committed
 *	  to the dataset with the transaction, and DROP INDEX drops it.  The
 *	  index has no storage and no scan of its own: Lance uses the dataset's
 *	  indices itself -- a search LanceScan the vector index, a filter the
 *	  scalar ones -- and so does any other reader of the dataset; the
 *	  planner costs a LanceScan by the BTREE, BITMAP and LABEL_LIST ones that
 *	  answer its quals (customscan.c).  Rows added later are searched
 *	  without the index until VACUUM (or lance.optimize()) brings it up to
 *	  them.
 *
 *	  The index's options become the Lance index's parameters: "type", and
 *	  the others as the type takes them (metric, num_partitions,
 *	  num_sub_vectors, num_bits, m, ef_construction, max_iterations,
 *	  sample_rate, ... and, for an inverted index, its tokenizer's).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

#include "lance_tam.h"

bool
lt_is_lance_index_am(Oid amoid)
{
	static Oid	oid = InvalidOid;

	if (!OidIsValid(oid))
		oid = get_index_am_oid("lance_index", true);
	return OidIsValid(oid) && amoid == oid;
}

/* The options a lance_index takes: Lance's parameters, with "type" */
static const char *const index_options[] = {
	"type", "metric", "distance_type", "num_partitions", "target_partition_size",
	"num_sub_vectors", "num_bits", "max_iterations", "sample_rate", "m", "ef_construction",
	"with_position", "base_tokenizer", "language", "lower_case", "stem", "remove_stop_words",
	"ascii_folding", "max_token_length", "min_ngram_length", "max_ngram_length",
	"prefix_only", "params", NULL
};

static bool
looks_numeric(const char *s)
{
	char	   *end;

	if (*s == '\0')
		return false;
	(void) strtod(s, &end);
	return *end == '\0';
}

/*
 * The options, checked, as the JSON object the engine takes: in rd_options
 * as a text value.  "params" is a JSON object merged in whole.
 */
static bytea *
lt_amoptions(Datum reloptions, bool validate)
{
	List	   *defs = untransformRelOptions(reloptions);
	ListCell   *lc;
	StringInfoData buf;
	bool		first = true;
	char	   *params = NULL;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');
	foreach(lc, defs)
	{
		DefElem    *d = lfirst(lc);
		char	   *value = d->arg ? defGetString(d) : "true";
		bool		known = false;

		for (int i = 0; index_options[i]; i++)
			if (strcmp(index_options[i], d->defname) == 0)
				known = true;
		if (!known && validate)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized parameter \"%s\" of a lance_index", d->defname)));
		if (strcmp(d->defname, "params") == 0)
		{
			params = value;
			continue;
		}
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		escape_json(&buf, d->defname);
		appendStringInfoChar(&buf, ':');
		if (looks_numeric(value) && strcmp(d->defname, "type") != 0)
			appendStringInfoString(&buf, value);
		else if (strcmp(value, "true") == 0 || strcmp(value, "false") == 0)
			appendStringInfoString(&buf, value);
		else
			escape_json(&buf, value);
	}
	if (params)
	{
		/* the members of a JSON object, appended as they are */
		char	   *p = params;
		size_t		len;

		while (*p == ' ' || *p == '\n' || *p == '\t')
			p++;
		len = strlen(p);
		while (len > 0 && (p[len - 1] == ' ' || p[len - 1] == '\n'))
			len--;
		if (len < 2 || p[0] != '{' || p[len - 1] != '}')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"params\" of a lance_index must be a JSON object")));
		if (len > 2)
		{
			if (!first)
				appendStringInfoChar(&buf, ',');
			appendBinaryStringInfo(&buf, p + 1, len - 2);
		}
	}
	appendStringInfoChar(&buf, '}');
	if (validate)
		(void) DirectFunctionCall1(json_in, CStringGetDatum(buf.data));
	return (bytea *) cstring_to_text(buf.data);
}

/* A JSON string member of the options, or NULL */
static char *
option_type(const char *json)
{
	const char *p = strstr(json, "\"type\":\"");
	const char *end;

	if (p == NULL)
		return NULL;
	p += strlen("\"type\":\"");
	end = strchr(p, '"');
	return end ? pnstrdup(p, end - p) : NULL;
}

static IndexBuildResult *
lt_ambuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result = palloc0(sizeof(IndexBuildResult));
	AttrNumber	attnum;
	LtTable    *t;
	char	   *json;
	char	   *type;
	Form_pg_attribute att;
	int			dim;
	bool		half;
	LtXactTable *xt;
	LeWrite    *w;
	LeDataset  *base;
	LeDatasetInfo info;
	LeError		err = {0};
	int			rc;

	if (!lt_is_lance_rel(heap))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("a lance_index can only index a lance table")));
	if (indexInfo->ii_NumIndexAttrs != 1 || indexInfo->ii_IndexAttrNumbers[0] == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a lance_index indexes one column, not an expression")));
	if (indexInfo->ii_Predicate != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a lance_index cannot be partial")));
	attnum = indexInfo->ii_IndexAttrNumbers[0];
	t = lt_table(heap);
	if (attnum > t->natts || t->columns[attnum - 1] == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("the indexed column is not a column of the table's dataset")));
	json = index->rd_options ? text_to_cstring((text *) index->rd_options) : pstrdup("{}");
	type = option_type(json);
	att = TupleDescAttr(RelationGetDescr(heap), attnum - 1);
	if (type == NULL)
		type = lt_type_is_vector(getBaseType(att->atttypid), &dim, &half) ? "IVF_PQ" : "BTREE";

	xt = lt_xact_table(heap, true);
	w = lt_xact_write(heap, xt);
	lt_xact_flush(xt);
	base = lt_open_dataset(t, lt_catalog_version(RelationGetRelid(heap), NULL));
	PG_TRY();
	{
		memset(&info, 0, sizeof(info));
		if (lt_api()->dataset_info(base, &info, &err) != LE_OK)
			lt_raise(&err, "reading the dataset to index");
		rc = lt_api()->write_create_index(w, base, t->columns[attnum - 1], type, json,
										  RelationGetRelationName(index), true,
										  GetCurrentSubTransactionId(), &err);
		if (rc != LE_OK)
			lt_raise(&err, "building a Lance index");
	}
	PG_FINALLY();
	{
		lt_api()->dataset_close(base);
	}
	PG_END_TRY();
	result->heap_tuples = (double) info.num_rows;
	result->index_tuples = (double) info.num_rows;
	return result;
}

static void
lt_ambuildempty(Relation index)
{
}

static bool
lt_aminsert(Relation index, Datum *values, bool *isnull, ItemPointer ht_ctid, Relation heap,
			IndexUniqueCheck checkUnique, bool indexUnchanged, IndexInfo *indexInfo)
{
	/* Lance indexes new rows when its index is optimized */
	return false;
}

static IndexBulkDeleteResult *
lt_ambulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				IndexBulkDeleteCallback callback, void *callback_state)
{
	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));
	return stats;
}

static IndexBulkDeleteResult *
lt_amvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));
	return stats;
}

/* Never a path of its own: LanceScan searches the dataset's indices */
static void
lt_amcostestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost,
				  Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation,
				  double *indexPages)
{
	*indexStartupCost = 1.0e10;
	*indexTotalCost = 1.0e10;
	*indexSelectivity = 1.0;
	*indexCorrelation = 0;
	*indexPages = 1;
}

static bool
lt_amvalidate(Oid opclassoid)
{
	return true;
}

static IndexScanDesc
lt_ambeginscan(Relation index, int nkeys, int norderbys)
{
	return RelationGetIndexScan(index, nkeys, norderbys);
}

static void
lt_amrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
}

static void
lt_amendscan(IndexScanDesc scan)
{
}

PG_FUNCTION_INFO_V1(lance_indexam_handler);

Datum
lance_indexam_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *am = makeNode(IndexAmRoutine);

	am->amstrategies = 0;
	am->amsupport = 0;
	am->amoptsprocnum = 0;
	am->amcanorder = false;
	am->amcanorderbyop = false;
	am->amcanhash = false;
	am->amconsistentequality = false;
	am->amconsistentordering = false;
	am->amcanbackward = false;
	am->amcanunique = false;
	am->amcanmulticol = false;
	am->amoptionalkey = true;
	am->amsearcharray = false;
	am->amsearchnulls = false;
	am->amstorage = false;
	am->amclusterable = false;
	am->ampredlocks = false;
	am->amcanparallel = false;
	am->amcanbuildparallel = false;
	am->amcaninclude = false;
	am->amusemaintenanceworkmem = false;
	am->amsummarizing = false;
	am->amparallelvacuumoptions = 0;
	am->amkeytype = InvalidOid;

	am->ambuild = lt_ambuild;
	am->ambuildempty = lt_ambuildempty;
	am->aminsert = lt_aminsert;
	am->aminsertcleanup = NULL;
	am->ambulkdelete = lt_ambulkdelete;
	am->amvacuumcleanup = lt_amvacuumcleanup;
	am->amcanreturn = NULL;
	am->amcostestimate = lt_amcostestimate;
	am->amgettreeheight = NULL;
	am->amoptions = lt_amoptions;
	am->amproperty = NULL;
	am->ambuildphasename = NULL;
	am->amvalidate = lt_amvalidate;
	am->amadjustmembers = NULL;
	am->ambeginscan = lt_ambeginscan;
	am->amrescan = lt_amrescan;
	am->amgettuple = NULL;
	am->amgetbitmap = NULL;
	am->amendscan = lt_amendscan;
	am->ammarkpos = NULL;
	am->amrestrpos = NULL;
	am->amestimateparallelscan = NULL;
	am->aminitparallelscan = NULL;
	am->amparallelrescan = NULL;
	am->amtranslatestrategy = NULL;
	am->amtranslatecmptype = NULL;

	PG_RETURN_POINTER(am);
}
