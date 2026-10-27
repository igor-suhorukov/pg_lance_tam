/*-------------------------------------------------------------------------
 *
 * customscan.c
 *	  LanceScan (docs/design.md §2.8): the extension's CustomScan over a
 *	  lance table, offered at set_rel_pathlist_hook beside the sequential
 *	  scan, which reads every column.
 *
 *	  A plain LanceScan reads the columns its target and quals name, and
 *	  gives the quals Lance can decide to Lance's scanner as a predicate
 *	  (filter.c), which Lance's scalar indices answer where the dataset has
 *	  them; the executor evaluates every qual again on what is read.  A qual
 *	  may compare a column with a value the scan is given when it starts --
 *	  a parameter of a generic plan, an InitPlan's result, a stable
 *	  expression of them, the outer row's value of a nested loop -- and its
 *	  predicate is made of the values each time the scan starts.
 *
 *	  It is costed as Lance runs it: with no qual Lance decides, as a
 *	  sequential scan of the columns it reads; with quals, as that read
 *	  filtered by Lance, PostgreSQL's per-row work only for the rows Lance
 *	  yields; and with quals a BTREE or BITMAP index of the dataset answers
 *	  (lt_version_info), as the index's searches, the reads of the rows they
 *	  find -- the pages those are on, as an index scan's of heap pages, the
 *	  nearer to a sequential read the more the column's order is the
 *	  dataset's -- and the filtered read of the rows no index covers.  For a
 *	  join clause such an index answers, a LanceScan parameterized by the
 *	  clause's outer relations is offered too, for a nested loop that
 *	  searches the index for each outer row; each of its scans pays
 *	  lance.scan_start_cost.
 *
 *	  A plain LanceScan that no index answers is offered as a partial path
 *	  too, for a parallel plan: its participants take the version's
 *	  fragments one at a time from the access method's parallel scan
 *	  (tableam.c), a reader of the scan's columns and filter for each.  The
 *	  CPU's work is shared among them and the reads are not, as a parallel
 *	  SeqScan's, and no more workers than fragments are planned.
 *
 *	  A search LanceScan answers ORDER BY v <op> q LIMIT k -- v a vector
 *	  column (pgvector's vector or halfvec), q a value known at the scan's
 *	  start, <op> pgvector's <-> (L2), <=> (cosine) or <#> (negative inner
 *	  product) -- with Lance's nearest-neighbour search, through the
 *	  column's vector index when the dataset has one and by a flat search
 *	  otherwise, and hands the rows in the order of their distance.  With
 *	  every qual given to Lance, the search filters first and yields k
 *	  rows; else it fetches more and the executor's quals may leave fewer,
 *	  as an approximate index scan of pgvector's may.  It is offered for a
 *	  query of one relation whose LIMIT the planner knows.
 *
 *	  A writer's: ModifyTable fetches an UPDATE's old row by its TID, and a
 *	  DELETE's for RETURNING and row triggers; a LanceScan of a relation the
 *	  query writes or locks reads every column, so that the fetch takes the
 *	  row from the scan's batch (scan.c).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_statistic.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/ruleutils.h"
#include "utils/selfuncs.h"
#include "varatt.h"

#include "lance_tam.h"

static set_rel_pathlist_hook_type prev_set_rel_pathlist = NULL;
static build_simple_rel_hook_type prev_build_simple_rel = NULL;

static Plan *lancescan_plan(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
							List *tlist, List *clauses, List *custom_plans);
static Node *lancescan_create_state(CustomScan *cscan);
static void lancescan_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *lancescan_exec(CustomScanState *node);
static void lancescan_end(CustomScanState *node);
static void lancescan_rescan(CustomScanState *node);
static void lancescan_explain(CustomScanState *node, List *ancestors, ExplainState *es);
static Size lancescan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt);
static void lancescan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt,
									 void *coordinate);
static void lancescan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt,
									   void *coordinate);
static void lancescan_initialize_worker(CustomScanState *node, shm_toc *toc, void *coordinate);

static const CustomPathMethods lancescan_path_methods = {
	.CustomName = "LanceScan",
	.PlanCustomPath = lancescan_plan,
};

static const CustomScanMethods lancescan_scan_methods = {
	.CustomName = "LanceScan",
	.CreateCustomScanState = lancescan_create_state,
};

static const CustomExecMethods lancescan_exec_methods = {
	.CustomName = "LanceScan",
	.BeginCustomScan = lancescan_begin,
	.ExecCustomScan = lancescan_exec,
	.EndCustomScan = lancescan_end,
	.ReScanCustomScan = lancescan_rescan,
	.EstimateDSMCustomScan = lancescan_estimate_dsm,
	.InitializeDSMCustomScan = lancescan_initialize_dsm,
	.ReInitializeDSMCustomScan = lancescan_reinitialize_dsm,
	.InitializeWorkerCustomScan = lancescan_initialize_worker,
	.ExplainCustomScan = lancescan_explain,
};

/*
 * custom_private: what the plan carries to the executor.  A path's has the
 * same members, the filter's left empty for lancescan_plan(), and a
 * search's query vector after them, which the plan moves to custom_exprs.
 */
enum
{
	PRIV_ATTNUMS,				/* IntList: the attributes read, NIL: every
								 * one */
	PRIV_FILTER,				/* String: the Lance predicate of the quals
								 * of constants, "" for none */
	PRIV_NN_ATTNUM,				/* Integer: the vector column, 0: no search */
	PRIV_NN_K,					/* Integer: the rows the search asks for */
	PRIV_NN_METRIC,				/* Integer: LE_METRIC_* */
	PRIV_NN_HALF,				/* Integer: 1 for halfvec */
	PRIV_NN_PREFILTER,			/* Integer: 1 when every qual is Lance's */
	PRIV_NN_OPNAME,				/* String: the operator, for EXPLAIN */
	PRIV_RT_PIECES,				/* List: each qual with values' pieces
								 * (filter.c) */
	PRIV_RT_INFO,				/* List: each value's IntList; the values
								 * are the last of custom_exprs */
	PRIV_INDICES,				/* List of Strings: the Lance indices the cost
								 * counts on, for EXPLAIN */
	PRIV_COUNT
};

/* lance.scan_start_cost */
double		lt_scan_start_cost = 100.0;

typedef struct LanceScanState
{
	CustomScanState css;
	LtReader   *reader;
	List	   *attnums;
	char	   *filter;
	int			nn_attnum;
	int			nn_k;
	int			nn_metric;
	bool		nn_half;
	bool		nn_prefilter;
	char	   *nn_opname;
	List	   *indices;
	ExprState  *query_state;
	/* the quals with values, and the values, evaluated as the scan starts */
	List	   *rt_pieces;
	List	   *rt_info;
	int			rt_nvalues;
	ExprState **rt_states;
	Datum	   *rt_values;
	bool	   *rt_nulls;
	bool		never;			/* the values leave no row a qual admits */
	MemoryContext scan_cxt;		/* a scan's filter and query, reset as the
								 * next starts */
	LtReadSpec	spec;			/* what a scan reads, in scan_cxt */
	TableScanDesc pscan;		/* a parallel scan's participant: the access
								 * method's scan it takes fragments from */
	bool		started;
	uint64		rows;
	uint64		scans;
	uint64		batches;		/* the ended scans' */
} LanceScanState;

/*
 * What Lance does of a scan's quals, as the planner costs it: the ones it
 * filters by, and of those the ones a BTREE or BITMAP index answers.
 */
typedef struct LanceQuals
{
	List	   *pushed;			/* RestrictInfos */
	List	   *indexed;
	int			nsearch;		/* the index searches they take */
	double		covered;		/* the rows of the least covering index */
	AttrNumber	attnum;			/* a column they compare, for its order */
	List	   *index_names;	/* Strings */
} LanceQuals;

/* The attributes a scan of the relation must read, or NIL for every one */
static List *
needed_attnums(RelOptInfo *rel, Index rti, List *quals, bool *all)
{
	Bitmapset  *attrs = NULL;
	ListCell   *lc;
	List	   *out = NIL;
	int			k = -1;

	pull_varattnos((Node *) rel->reltarget->exprs, rti, &attrs);
	foreach(lc, quals)
		pull_varattnos((Node *) ((RestrictInfo *) lfirst(lc))->clause, rti, &attrs);
	*all = false;
	while ((k = bms_next_member(attrs, k)) >= 0)
	{
		AttrNumber	attno = k + FirstLowInvalidHeapAttributeNumber;

		if (attno == 0)
		{
			*all = true;
			list_free(out);
			return NIL;
		}
		if (attno > 0)
			out = lappend_int(out, attno);
	}
	if (out == NIL)
	{
		/* count(*): no column, but a scan needs one to count rows by */
		*all = false;
	}
	return out;
}

/* Whether the query writes or locks the relation */
static bool
written_or_locked(PlannerInfo *root, Index rti)
{
	ListCell   *lc;

	if (root->parse->resultRelation == (int) rti)
		return true;
	foreach(lc, root->rowMarks)
	{
		PlanRowMark *rc = lfirst(lc);

		if (rc->rti == rti)
			return true;
	}
	return false;
}

static double
attr_width(Relation relation, AttrNumber attno)
{
	Form_pg_attribute att = TupleDescAttr(RelationGetDescr(relation), attno - 1);
	int32		w;

	if (att->attisdropped)
		return 0;
	if (att->attlen > 0)
		return att->attlen;
	w = get_attavgwidth(RelationGetRelid(relation), attno);
	if (w > 0)
		return w;
	return get_typavgwidth(att->atttypid, att->atttypmod);
}

static double
read_fraction(Relation relation, List *attnums, bool all)
{
	TupleDesc	desc = RelationGetDescr(relation);
	double		total = 0;
	double		read = 0;
	ListCell   *lc;

	for (int i = 1; i <= desc->natts; i++)
		total += attr_width(relation, i);
	if (all || total <= 0)
		return 1.0;
	foreach(lc, attnums)
		read += attr_width(relation, lfirst_int(lc));
	return Max(Min(read / total, 1.0), 0.02);
}

/* The dataset's BTREE or BITMAP index of the attribute, the widest, or NULL */
static LtIndexInfo *
scalar_index(LtIndexInfo *indices, int nindices, AttrNumber attnum)
{
	LtIndexInfo *best = NULL;

	for (int i = 0; i < nindices; i++)
	{
		if (indices[i].attnum == attnum &&
			(indices[i].kind == LT_INDEX_BTREE || indices[i].kind == LT_INDEX_BITMAP) &&
			(best == NULL || indices[i].rows > best->rows))
			best = &indices[i];
	}
	return best;
}

/* Whether an index answers a qual: all its columns' indexed */
static bool
qual_indexed(LtFilterClause *fc, LtIndexInfo *indices, int nindices, double *covered,
			 List **names)
{
	int			k = -1;

	if (!fc->sargable || bms_is_empty(fc->attnums))
		return false;
	while ((k = bms_next_member(fc->attnums, k)) >= 0)
	{
		LtIndexInfo *idx = scalar_index(indices, nindices, (AttrNumber) k);

		if (idx == NULL)
			return false;
		if (covered)
			*covered = Min(*covered, idx->rows);
		if (names)
			*names = list_append_unique(*names, makeString(pstrdup(idx->name)));
	}
	return true;
}

/*
 * The quals Lance decides, their values of the Vars of outer_relids among
 * them, and of those the ones an index answers.
 */
static void
divide_quals(PlannerInfo *root, RelOptInfo *rel, Relation relation, LtTable *t, List *quals,
			 Relids outer_relids, LtIndexInfo *indices, int nindices, LanceQuals *lq)
{
	ListCell   *lc;

	memset(lq, 0, sizeof(*lq));
	lq->covered = rel->tuples;
	foreach(lc, quals)
	{
		RestrictInfo *ri = lfirst(lc);
		LtFilterClause *fc = lt_filter_clause(root, relation, t, rel->relid, (Node *) ri,
											  outer_relids, 0);
		double		covered = rel->tuples;

		if (fc == NULL)
			continue;
		lq->pushed = lappend(lq->pushed, ri);
		if (!qual_indexed(fc, indices, nindices, &covered, &lq->index_names))
			continue;
		lq->indexed = lappend(lq->indexed, ri);
		lq->nsearch += fc->nsearch;
		lq->covered = Min(lq->covered, covered);
		if (lq->attnum == 0)
			lq->attnum = (AttrNumber) bms_next_member(fc->attnums, -1);
	}
}

/* The correlation ANALYZE found of a column's order with the dataset's */
static double
column_correlation(PlannerInfo *root, RelOptInfo *rel, Relation relation, AttrNumber attnum)
{
	Form_pg_attribute att = TupleDescAttr(RelationGetDescr(relation), attnum - 1);
	Var		   *var = makeVar(rel->relid, attnum, att->atttypid, att->atttypmod,
							  att->attcollation, 0);
	VariableStatData vardata;
	double		correlation = 0;

	examine_variable(root, (Node *) var, rel->relid, &vardata);
	if (HeapTupleIsValid(vardata.statsTuple))
	{
		AttStatsSlot sslot;

		if (get_attstatsslot(&sslot, vardata.statsTuple, STATISTIC_KIND_CORRELATION, InvalidOid,
							 ATTSTATSSLOT_NUMBERS))
		{
			if (sslot.nnumbers > 0)
				correlation = sslot.numbers[0];
			free_attstatsslot(&sslot);
		}
	}
	ReleaseVariableStats(vardata);
	return correlation;
}

/*
 * A plain scan's cost: its start, its reads, and the CPU's work, which a
 * parallel scan's participants share while the reads they do not, as a
 * parallel SeqScan's.  The quals Lance filters by are evaluated in its
 * batches, a row at PostgreSQL's cost of the quals but without its cost of
 * a row; the rows an index finds are read where they are, as an index scan
 * reads a heap's pages.
 */
typedef struct ScanCost
{
	Cost		startup;
	Cost		io;
	Cost		cpu;
	int			disabled;
} ScanCost;

static void
scan_cost(PlannerInfo *root, RelOptInfo *rel, Relation relation, List *quals, LanceQuals *lq,
		  double fraction, ScanCost *c)
{
	double		tuples = rel->tuples;
	double		pages = Max(rel->pages * fraction, 1.0);
	QualCost	qcost;
	QualCost	pcost;
	Cost		cpu_per_tuple;
	double		yielded;
	double		selectivity;
	double		covered;
	double		share;
	double		found;
	double		fetched;
	double		max_io;
	double		min_io;
	double		csquared;

	cost_qual_eval(&qcost, extract_actual_clauses(quals, false), root);
	cpu_per_tuple = cpu_tuple_cost + qcost.per_tuple;
	c->startup = qcost.startup;
	c->disabled = 0;
	if (lq->pushed == NIL)
	{
		/*
		 * A sequential scan of the columns read, a hair below a SeqScan's,
		 * and disabled as a SeqScan is.
		 */
		c->io = 0.999 * seq_page_cost * rel->pages * fraction;
		c->cpu = 0.999 * cpu_per_tuple * tuples;
		c->disabled = enable_seqscan ? 0 : 1;
		return;
	}

	/* Lance's read and filter, then PostgreSQL's quals on what it yields */
	cost_qual_eval(&pcost, extract_actual_clauses(lq->pushed, false), root);
	c->startup += pcost.startup;
	yielded = clamp_row_est(tuples * clauselist_selectivity(root, lq->pushed, rel->relid,
															JOIN_INNER, NULL));
	if (lq->indexed == NIL)
	{
		c->io = seq_page_cost * pages;
		c->cpu = pcost.per_tuple * tuples + cpu_per_tuple * yielded;
		/* never above reading every row for PostgreSQL to filter */
		if (c->io + c->cpu > 0.999 * (seq_page_cost * rel->pages * fraction + cpu_per_tuple * tuples))
		{
			c->io = 0.999 * seq_page_cost * rel->pages * fraction;
			c->cpu = 0.999 * cpu_per_tuple * tuples;
		}
		c->disabled = enable_seqscan ? 0 : 1;
		return;
	}

	selectivity = clauselist_selectivity(root, lq->indexed, rel->relid, JOIN_INNER, NULL);
	covered = Min(lq->covered, tuples);
	share = tuples > 0 ? covered / tuples : 0;
	found = covered * selectivity;

	/* the searches, each a page of the index, and the entries they find */
	c->startup += random_page_cost * Min(lq->nsearch, 1000);
	c->cpu = cpu_operator_cost * lq->nsearch * log2(covered + 2) + cpu_index_tuple_cost * found;

	/* the pages the rows found are on, between scattered and in a run */
	c->io = 0;
	if (found >= 1)
	{
		fetched = index_pages_fetched(found, (BlockNumber) Min(ceil(pages * share), (double) MaxBlockNumber),
									  0, root);
		max_io = fetched * random_page_cost;
		fetched = ceil(selectivity * pages * share);
		min_io = random_page_cost + Max(fetched - 1, 0) * seq_page_cost;
		csquared = column_correlation(root, rel, relation, lq->attnum);
		csquared *= csquared;
		c->io += max_io + csquared * (min_io - max_io);
	}

	/* the rest of the filter on them, the rows no index covers, and PostgreSQL's quals */
	c->cpu += pcost.per_tuple * found;
	c->io += (1 - share) * seq_page_cost * pages;
	c->cpu += (1 - share) * pcost.per_tuple * tuples;
	c->cpu += cpu_per_tuple * yielded;
}

/* get_parallel_divisor()'s, costsize.c: the leader's share of the work counted */
static double
parallel_divisor(int workers)
{
	double		divisor = workers;
	double		leader = 1.0 - 0.3 * workers;

	if (parallel_leader_participation && leader > 0)
		divisor += leader;
	return divisor;
}

/*
 * A LanceScan's path: a search's with pathkeys, else a plain scan's; with
 * workers, a parallel scan's, each participant's share of it.
 */
static CustomPath *
make_path(PlannerInfo *root, RelOptInfo *rel, Relation relation, List *private, double fraction,
		  List *pathkeys, double rows, Cost startup_extra, ParamPathInfo *ppi, List *quals,
		  LanceQuals *lq, int workers)
{
	CustomPath *path = makeNode(CustomPath);

	path->path.pathtype = T_CustomScan;
	path->path.parent = rel;
	path->path.pathtarget = rel->reltarget;

	/*
	 * A relation LATERAL references are made of is scanned under its outer
	 * relations, as a sequential scan's path is: parameterized by them, so
	 * that the planner replaces their Vars with nestloop parameters; and so
	 * is a scan for a join clause an index answers, by the clause's.
	 */
	path->path.param_info = ppi;
	if (ppi)
		rows = Min(rows, ppi->ppi_rows);
	path->path.parallel_aware = false;
	path->path.parallel_safe = rel->consider_parallel;
	path->path.parallel_workers = 0;
	path->path.rows = rows;
	path->path.pathkeys = pathkeys;
	if (pathkeys != NIL)
	{
		/* a search: the rows it yields, after the search itself */
		QualCost	qcost;

		cost_qual_eval(&qcost, extract_actual_clauses(quals, false), root);
		path->path.startup_cost = qcost.startup + startup_extra;
		path->path.total_cost = path->path.startup_cost +
			rows * (cpu_tuple_cost + qcost.per_tuple);
	}
	else
	{
		ScanCost	c;

		scan_cost(root, rel, relation, quals, lq, fraction, &c);
		path->path.startup_cost = c.startup + startup_extra;
		path->path.disabled_nodes = c.disabled;
		if (workers > 0)
		{
			double		divisor = parallel_divisor(workers);

			/*
			 * The CPU's work shared, the reads not.  Each fragment is a scan
			 * Lance starts, as each is of the access method's parallel scan,
			 * which PostgreSQL costs without them; neither is charged them.
			 */
			path->path.parallel_aware = true;
			path->path.parallel_workers = workers;
			path->path.rows = clamp_row_est(rows / divisor);
			path->path.total_cost = path->path.startup_cost + c.io + c.cpu / divisor;
		}
		else
			path->path.total_cost = path->path.startup_cost + c.io + c.cpu;
	}
	if (ppi)
	{
		/* each scan of a nested loop's is a scan Lance starts */
		path->path.startup_cost += lt_scan_start_cost;
		path->path.total_cost += lt_scan_start_cost;
	}
	path->flags = 0;
	path->custom_paths = NIL;
	path->custom_private = private;
	path->methods = &lancescan_path_methods;
	return path;
}

/*
 * The ORDER BY of a search: one pathkey, ascending, whose expression is
 * v <op> q with v a vector column of the relation and q pseudo-constant.
 */
static bool
match_search(PlannerInfo *root, RelOptInfo *rel, Relation relation, AttrNumber *attnum,
			 int *metric, bool *half, Expr **query, char **opname)
{
	PathKey    *pk;
	ListCell   *lc;

	if (list_length(root->query_pathkeys) != 1)
		return false;
	pk = linitial(root->query_pathkeys);
	if (pk->pk_cmptype != COMPARE_LT || pk->pk_nulls_first)
		return false;
	foreach(lc, pk->pk_eclass->ec_members)
	{
		EquivalenceMember *em = lfirst(lc);
		OpExpr	   *op;
		Node	   *a;
		Node	   *b;
		Var		   *var;
		char	   *name;
		int			dim;
		bool		is_half = false;

		if (!IsA(em->em_expr, OpExpr))
			continue;
		op = (OpExpr *) em->em_expr;
		if (list_length(op->args) != 2)
			continue;
		name = get_opname(op->opno);
		if (name == NULL)
			continue;
		if (strcmp(name, "<->") == 0)
			*metric = LE_METRIC_L2;
		else if (strcmp(name, "<=>") == 0)
			*metric = LE_METRIC_COSINE;
		else if (strcmp(name, "<#>") == 0)
			*metric = LE_METRIC_DOT;
		else
			continue;
		a = linitial(op->args);
		b = lsecond(op->args);
		if (!IsA(a, Var))
		{
			Node	   *t = a;

			a = b;
			b = t;
		}
		if (!IsA(a, Var))
			continue;
		var = (Var *) a;
		if (var->varno != rel->relid || var->varattno <= 0 || var->varlevelsup != 0)
			continue;
		if (!lt_type_is_vector(getBaseType(var->vartype), &dim, &is_half))
			continue;
		if (contain_var_clause(b) || contain_volatile_functions(b) ||
			exprType(b) != var->vartype)
			continue;
		*attnum = var->varattno;
		*half = is_half;
		*query = (Expr *) b;
		*opname = name;
		return true;
	}
	return false;
}

static bool
search_allowed(PlannerInfo *root)
{
	Query	   *parse = root->parse;

	return lt_enable_ann && root->limit_tuples > 0 &&
		bms_membership(root->all_baserels) == BMS_SINGLETON &&
		parse->groupClause == NIL && !parse->hasAggs && parse->distinctClause == NIL &&
		!parse->hasWindowFuncs && !parse->hasTargetSRFs && parse->setOperations == NULL &&
		parse->groupingSets == NIL && parse->havingQual == NULL;
}

static List *
make_private(List *attnums, int nn_attnum, int nn_k, int nn_metric, bool nn_half,
			 bool nn_prefilter, const char *nn_opname, List *index_names)
{
	List	   *private;

	private = list_make5(attnums, makeString(pstrdup("")), makeInteger(nn_attnum),
						 makeInteger(nn_k), makeInteger(nn_metric));
	private = lappend(private, makeInteger(nn_half ? 1 : 0));
	private = lappend(private, makeInteger(nn_prefilter ? 1 : 0));
	private = lappend(private, makeString(pstrdup(nn_opname)));
	private = lappend(private, NIL);
	private = lappend(private, NIL);
	private = lappend(private, index_names);
	return private;
}

/* The attributes read with those the quals name, unless every one is */
static List *
attnums_with(List *attnums, bool all, List *quals, Index rti)
{
	Bitmapset  *attrs = NULL;
	ListCell   *lc;
	int			k = -1;

	if (all)
		return NIL;
	foreach(lc, quals)
		pull_varattnos((Node *) ((RestrictInfo *) lfirst(lc))->clause, rti, &attrs);
	attnums = list_copy(attnums);
	while ((k = bms_next_member(attrs, k)) >= 0)
	{
		AttrNumber	attno = k + FirstLowInvalidHeapAttributeNumber;

		if (attno > 0)
			attnums = list_append_unique_int(attnums, attno);
	}
	return attnums;
}

/* generate_implied_equalities_for_column()'s: an indexed column's members */
typedef struct EcMemberArg
{
	Expr	   *current;
	List	   *already_used;
	LtIndexInfo *indices;
	int			nindices;
} EcMemberArg;

static bool
ec_member_matches(PlannerInfo *root, RelOptInfo *rel, EquivalenceClass *ec,
				  EquivalenceMember *em, void *arg)
{
	EcMemberArg *a = (EcMemberArg *) arg;
	Expr	   *expr = em->em_expr;
	Expr	   *var = expr;

	if (a->current != NULL)
		return equal(expr, a->current);
	if (var && IsA(var, RelabelType))
		var = ((RelabelType *) var)->arg;
	if (var == NULL || !IsA(var, Var) || ((Var *) var)->varno != rel->relid ||
		((Var *) var)->varattno <= 0 ||
		scalar_index(a->indices, a->nindices, ((Var *) var)->varattno) == NULL)
		return false;
	if (list_member(a->already_used, expr))
		return false;
	a->current = expr;
	return true;
}

/* A join clause an index answers: the parameterization by its outer relations */
static void
consider_join_clause(PlannerInfo *root, RelOptInfo *rel, Relation relation, LtTable *t,
					 LtIndexInfo *indices, int nindices, RestrictInfo *ri, List **ppis)
{
	Relids		required_outer;
	LtFilterClause *fc;

	if (!join_clause_is_movable_to(ri, rel))
		return;
	required_outer = bms_union(ri->clause_relids, rel->lateral_relids);
	required_outer = bms_del_member(required_outer, rel->relid);
	if (bms_is_empty(required_outer))
		return;
	fc = lt_filter_clause(root, relation, t, rel->relid, (Node *) ri, required_outer, 0);
	if (fc == NULL || !qual_indexed(fc, indices, nindices, NULL, NULL))
		return;
	*ppis = list_append_unique_ptr(*ppis, get_baserel_parampathinfo(root, rel, required_outer));
}

/*
 * The parameterizations of a scan that searches an index for each outer
 * row: those of the join clauses an index answers, as postgres_fdw finds a
 * foreign scan's -- the generic join clauses and the equivalence classes'.
 */
static List *
index_join_ppis(PlannerInfo *root, RelOptInfo *rel, Relation relation, LtTable *t,
				LtIndexInfo *indices, int nindices)
{
	List	   *ppis = NIL;
	ListCell   *lc;

	foreach(lc, rel->joininfo)
		consider_join_clause(root, rel, relation, t, indices, nindices, lfirst(lc), &ppis);
	if (rel->has_eclass_joins)
	{
		EcMemberArg arg;

		memset(&arg, 0, sizeof(arg));
		arg.indices = indices;
		arg.nindices = nindices;
		for (;;)
		{
			List	   *clauses;

			arg.current = NULL;
			clauses = generate_implied_equalities_for_column(root, rel, ec_member_matches, &arg,
															 rel->lateral_referencers);
			if (arg.current == NULL)
				break;
			foreach(lc, clauses)
				consider_join_clause(root, rel, relation, t, indices, nindices, lfirst(lc),
									 &ppis);
			arg.already_used = lappend(arg.already_used, arg.current);
		}
	}
	return ppis;
}

static void
lt_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Relation	relation;
	LtTable    *t;
	List	   *attnums;
	bool		all;
	double		fraction;
	LtXactTable *xt;
	ListCell   *lc;
	LtVersionInfo *vinfo = NULL;
	LtIndexInfo *indices = NULL;
	int			nindices = 0;
	ParamPathInfo *ppi;
	List	   *quals;
	LanceQuals	lq;

	if (prev_set_rel_pathlist)
		prev_set_rel_pathlist(root, rel, rti, rte);
	/*
	 * An inheritance parent's rel is its tree's Append, whose target holds
	 * the row identity's placeholders; the parent's own rows are scanned by
	 * its entry as a child.
	 */
	if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION || rte->tablesample ||
		rte->inh ||
		(rel->reloptkind != RELOPT_BASEREL && rel->reloptkind != RELOPT_OTHER_MEMBER_REL))
		return;
	if (!lt_is_lance_relid(rte->relid))
		return;

	/*
	 * A transaction's own rows are in its backend: no parallel worker sees
	 * them, scanning a share of the table or all of it.
	 */
	xt = lt_xact_table_by_relid(rte->relid);
	if (xt && xt->write)
	{
		rel->partial_pathlist = NIL;
		rel->rel_parallel_workers = 0;
		rel->consider_parallel = false;
		foreach(lc, rel->pathlist)
			((Path *) lfirst(lc))->parallel_safe = false;
	}
	if (!lt_enable_customscan)
		return;

	/* WHERE CURRENT OF: the TID scan PostgreSQL makes is the only path */
	foreach(lc, rel->baserestrictinfo)
	{
		if (IsA(((RestrictInfo *) lfirst(lc))->clause, CurrentOfExpr))
			return;
	}

	relation = table_open(rte->relid, NoLock);
	t = lt_table(relation);
	attnums = needed_attnums(rel, rti, rel->baserestrictinfo, &all);
	if (written_or_locked(root, rti))
	{
		all = true;
		attnums = NIL;
	}
	if (all)
		attnums = NIL;
	fraction = read_fraction(relation, attnums, all);
	/*
	 * The version's indices, for a scan with quals or join clauses they might
	 * answer, and its fragments, for a parallel scan
	 */
	if ((lt_enable_filter_pushdown &&
		 (rel->baserestrictinfo != NIL || rel->joininfo != NIL || rel->has_eclass_joins)) ||
		(rel->consider_parallel && max_parallel_workers_per_gather > 0))
	{
		vinfo = lt_version_info(relation, lt_snapshot_version(relation, t, NULL));
		indices = vinfo->indices;
		nindices = vinfo->nindices;
	}

	/* the plain scan, under the relations LATERAL references */
	ppi = get_baserel_parampathinfo(root, rel, rel->lateral_relids);
	quals = rel->baserestrictinfo;
	if (ppi)
		quals = list_concat_copy(quals, ppi->ppi_clauses);
	divide_quals(root, rel, relation, t, quals, rel->lateral_relids, indices, nindices, &lq);
	add_path(rel, (Path *) make_path(root, rel, relation,
									 make_private(attnums_with(attnums, all, quals, rti), 0, 0, 0,
												  false, false, "", lq.index_names),
									 fraction, NIL, rel->rows, 0, ppi, quals, &lq, 0));

	/*
	 * A parallel scan of it, its participants taking the version's fragments
	 * one at a time (tableam.c): for a read no index answers, of more
	 * fragments than one, by no more workers than there are fragments.
	 */
	if (rel->consider_parallel && ppi == NULL && lq.indexed == NIL && vinfo &&
		vinfo->nfragments > 1)
	{
		/*
		 * Workers by the table's pages, as a parallel SeqScan's: a scan's
		 * work is its rows', in Lance's decoding and PostgreSQL's per row,
		 * however few columns it reads.
		 */
		int			workers = compute_parallel_worker(rel, rel->pages, -1,
													  max_parallel_workers_per_gather);

		workers = (int) Min((double) workers,
							(double) vinfo->nfragments - (parallel_leader_participation ? 1 : 0));
		if (workers > 0)
			add_partial_path(rel, (Path *) make_path(root, rel, relation,
													 make_private(attnums_with(attnums, all, quals, rti),
																  0, 0, 0, false, false, "", NIL),
													 fraction, NIL, rel->rows, 0, NULL, quals, &lq,
													 workers));
	}

	if (search_allowed(root) && rel->reloptkind == RELOPT_BASEREL && bms_is_empty(rel->lateral_relids))
	{
		AttrNumber	attnum;
		int			metric;
		bool		half;
		Expr	   *query;
		char	   *opname;

		if (match_search(root, rel, relation, &attnum, &metric, &half, &query, &opname))
		{
			bool		prefilter = list_length(lq.pushed) == list_length(rel->baserestrictinfo);
			double		k = ceil(root->limit_tuples);
			int			ask;
			CustomPath *path;
			List	   *sattnums = attnums;

			ask = (int) Min(prefilter ? k : k * 10, (double) PG_INT32_MAX / 2);
			if (!list_member_int(attnums, attnum) && !all)
				sattnums = lappend_int(list_copy(attnums), attnum);
			path = make_path(root, rel, relation,
							 make_private(sattnums, attnum, ask, metric, half, prefilter, opname,
										  NIL),
							 fraction, root->query_pathkeys, Min(k, rel->rows),
							 cpu_operator_cost * rel->tuples * 0.05, NULL, rel->baserestrictinfo,
							 &lq, 0);
			/* the query vector, evaluated when the scan begins */
			path->custom_private = lappend(path->custom_private, query);
			add_path(rel, (Path *) path);
		}
	}

	/* scans for the join clauses an index answers, for nested loops */
	if (nindices > 0)
	{
		foreach(lc, index_join_ppis(root, rel, relation, t, indices, nindices))
		{
			ParamPathInfo *pi = lfirst(lc);
			LanceQuals	plq;
			List	   *pquals = list_concat_copy(rel->baserestrictinfo, pi->ppi_clauses);

			divide_quals(root, rel, relation, t, pquals, pi->ppi_req_outer, indices,
						 nindices, &plq);
			if (plq.indexed == NIL)
				continue;
			add_path(rel, (Path *) make_path(root, rel, relation,
											 make_private(attnums_with(attnums, all, pquals, rti),
														  0, 0, 0, false, false, "",
														  plq.index_names),
											 fraction, NIL, pi->ppi_rows, 0, pi, pquals, &plq, 0));
		}
	}
	table_close(relation, NoLock);
}

static Plan *
lancescan_plan(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
			   List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	List	   *private = list_copy(best_path->custom_private);
	List	   *exprs = NIL;
	ParamPathInfo *ppi = best_path->path.param_info;
	Relids		outer_relids = PATH_REQ_OUTER(&best_path->path);
	Relation	relation;
	LtTable    *t;
	List	   *quals;
	StringInfoData filter;
	List	   *rt_pieces = NIL;
	List	   *rt_info = NIL;
	List	   *values = NIL;
	ListCell   *lc;

	if (list_length(private) > PRIV_COUNT)
	{
		exprs = list_make1(llast(private));
		private = list_truncate(private, PRIV_COUNT);
	}

	/*
	 * The quals Lance decides, in the order the path was costed by: those of
	 * constants as one predicate, the others as pieces, their values the
	 * last of custom_exprs, where the outer relations' Vars become nestloop
	 * parameters.
	 */
	quals = rel->baserestrictinfo;
	if (ppi)
		quals = list_concat_copy(quals, ppi->ppi_clauses);
	relation = table_open(planner_rt_fetch(rel->relid, root)->relid, NoLock);
	t = lt_table(relation);
	initStringInfo(&filter);
	foreach(lc, quals)
	{
		LtFilterClause *fc = lt_filter_clause(root, relation, t, rel->relid, lfirst(lc),
											  outer_relids, list_length(values));

		if (fc == NULL)
			continue;
		if (fc->values == NIL)
		{
			if (filter.len > 0)
				appendStringInfoString(&filter, " AND ");
			(void) lt_filter_pieces_append(&filter, fc->pieces, NULL, NULL);
		}
		else
		{
			rt_pieces = lappend(rt_pieces, fc->pieces);
			rt_info = list_concat(rt_info, fc->value_info);
			values = list_concat(values, fc->values);
		}
	}
	table_close(relation, NoLock);
	lfirst(list_nth_cell(private, PRIV_FILTER)) = makeString(filter.data);
	lfirst(list_nth_cell(private, PRIV_RT_PIECES)) = rt_pieces;
	lfirst(list_nth_cell(private, PRIV_RT_INFO)) = rt_info;

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = extract_actual_clauses(clauses, false);
	cscan->scan.scanrelid = rel->relid;
	cscan->flags = best_path->flags;
	cscan->custom_plans = NIL;
	cscan->custom_exprs = list_concat(exprs, values);
	cscan->custom_private = private;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_relids = NULL;
	cscan->methods = &lancescan_scan_methods;
	return &cscan->scan.plan;
}

static Node *
lancescan_create_state(CustomScan *cscan)
{
	LanceScanState *s = (LanceScanState *) newNode(sizeof(LanceScanState), T_CustomScanState);
	List	   *private = cscan->custom_private;
	char	   *filter = strVal(list_nth(private, PRIV_FILTER));

	s->css.methods = &lancescan_exec_methods;
	/* the table's slots, with the system columns a row has */
	s->css.slotOps = lt_slot_ops_of();
	s->attnums = (List *) list_nth(private, PRIV_ATTNUMS);
	s->filter = filter[0] ? filter : NULL;
	s->nn_attnum = intVal(list_nth(private, PRIV_NN_ATTNUM));
	s->nn_k = intVal(list_nth(private, PRIV_NN_K));
	s->nn_metric = intVal(list_nth(private, PRIV_NN_METRIC));
	s->nn_half = intVal(list_nth(private, PRIV_NN_HALF)) != 0;
	s->nn_prefilter = intVal(list_nth(private, PRIV_NN_PREFILTER)) != 0;
	s->nn_opname = strVal(list_nth(private, PRIV_NN_OPNAME));
	s->rt_pieces = (List *) list_nth(private, PRIV_RT_PIECES);
	s->rt_info = (List *) list_nth(private, PRIV_RT_INFO);
	s->rt_nvalues = list_length(s->rt_info);
	s->indices = (List *) list_nth(private, PRIV_INDICES);
	return (Node *) s;
}

static void
lancescan_begin(CustomScanState *node, EState *estate, int eflags)
{
	LanceScanState *s = (LanceScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	int			first = list_length(cscan->custom_exprs) - s->rt_nvalues;

	if (first > 0)
		s->query_state = ExecInitExpr(linitial(cscan->custom_exprs), &node->ss.ps);
	if (s->rt_nvalues > 0)
	{
		s->rt_states = palloc(sizeof(ExprState *) * s->rt_nvalues);
		s->rt_values = palloc(sizeof(Datum) * s->rt_nvalues);
		s->rt_nulls = palloc(sizeof(bool) * s->rt_nvalues);
		for (int i = 0; i < s->rt_nvalues; i++)
			s->rt_states[i] = ExecInitExpr(list_nth(cscan->custom_exprs, first + i),
										   &node->ss.ps);
	}
	s->scan_cxt = AllocSetContextCreate(estate->es_query_cxt, "LanceScan",
										ALLOCSET_DEFAULT_SIZES);
}

/* pgvector's layouts, as convert.c reads them */
typedef struct PgVecHeader
{
	int32		vl_len_;
	int16		dim;
	int16		unused;
} PgVecHeader;

static bool
append_scan_value(StringInfo buf, int value, void *arg)
{
	LanceScanState *s = (LanceScanState *) arg;

	return lt_filter_value_append(buf, list_nth(s->rt_info, value), s->rt_values[value],
								  s->rt_nulls[value], &s->never);
}

/*
 * The scan's predicate: the quals of constants', and of the quals with
 * values those whose values Lance takes; *complete when every one is given.
 */
static char *
scan_filter(LanceScanState *s, bool *complete)
{
	ExprContext *econtext = s->css.ss.ps.ps_ExprContext;
	StringInfoData buf;
	ListCell   *lc;

	*complete = true;
	if (s->rt_pieces == NIL)
		return s->filter;
	for (int i = 0; i < s->rt_nvalues; i++)
		s->rt_values[i] = ExecEvalExprSwitchContext(s->rt_states[i], econtext, &s->rt_nulls[i]);
	initStringInfo(&buf);
	if (s->filter)
		appendStringInfoString(&buf, s->filter);
	foreach(lc, s->rt_pieces)
	{
		int			len = buf.len;

		if (buf.len > 0)
			appendStringInfoString(&buf, " AND ");
		if (!lt_filter_pieces_append(&buf, lfirst(lc), append_scan_value, s))
		{
			/* this qual stays PostgreSQL's alone */
			buf.len = len;
			buf.data[len] = '\0';
			*complete = false;
		}
		if (s->never)
			break;
	}
	return buf.len > 0 ? buf.data : NULL;
}

static void
lancescan_start(LanceScanState *s)
{
	LtReadSpec	spec;
	EState	   *estate = s->css.ss.ps.state;
	MemoryContext old;
	bool		complete;

	s->started = true;
	s->never = false;
	MemoryContextReset(s->scan_cxt);
	old = MemoryContextSwitchTo(s->scan_cxt);
	memset(&spec, 0, sizeof(spec));
	spec.attnums = s->attnums;
	spec.filter = scan_filter(s, &complete);
	spec.limit = -1;
	spec.with_rowid = true;
	if (s->never)
	{
		/* a value no row compares true with: nothing to read */
		MemoryContextSwitchTo(old);
		return;
	}
	if (s->nn_attnum > 0 && s->query_state)
	{
		ExprContext *econtext = s->css.ss.ps.ps_ExprContext;
		bool		isnull;
		Datum		q = ExecEvalExprSwitchContext(s->query_state, econtext, &isnull);

		if (!isnull)
		{
			struct varlena *v = PG_DETOAST_DATUM(q);
			PgVecHeader *h = (PgVecHeader *) v;

			spec.nn_attnum = s->nn_attnum;
			spec.nn_dim = h->dim;
			spec.nn_k = s->nn_k;
			spec.nn_metric = s->nn_metric;
			spec.prefilter = s->nn_prefilter;
			if (s->nn_prefilter && !complete)
			{
				/* a qual is PostgreSQL's alone: fetch more, as a postfilter does */
				spec.prefilter = false;
				spec.nn_k = (int) Min((double) s->nn_k * 10, (double) PG_INT32_MAX / 2);
			}
			if (s->nn_half)
			{
				spec.nn_query_half = palloc(sizeof(int16) * h->dim);
				memcpy(spec.nn_query_half, (char *) v + sizeof(PgVecHeader), sizeof(int16) * h->dim);
			}
			else
			{
				spec.nn_query = palloc(sizeof(float4) * h->dim);
				memcpy(spec.nn_query, (char *) v + sizeof(PgVecHeader), sizeof(float4) * h->dim);
			}
		}
	}
	s->spec = spec;
	if (s->pscan == NULL)
	{
		s->reader = lt_reader_begin(s->css.ss.ss_currentRelation, estate->es_snapshot, &spec);
		s->scans++;
	}
	MemoryContextSwitchTo(old);
}

static void end_reader(LanceScanState *s);

static TupleTableSlot *
lancescan_next(ScanState *ss)
{
	LanceScanState *s = (LanceScanState *) ss;
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;

	if (!s->started)
		lancescan_start(s);
	for (;;)
	{
		MemoryContext old;

		if (s->reader && lt_reader_next(s->reader, slot))
		{
			s->rows++;
			return slot;
		}
		if (s->pscan == NULL || s->never)
			return ExecClearTuple(slot);

		/* a parallel scan's participant: the next fragment no other has taken */
		end_reader(s);
		old = MemoryContextSwitchTo(s->scan_cxt);
		s->reader = lt_scan_take_reader(s->pscan, &s->spec);
		MemoryContextSwitchTo(old);
		if (s->reader == NULL)
			return ExecClearTuple(slot);
		s->scans++;
	}
}

static bool
lancescan_recheck(ScanState *ss, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
lancescan_exec(CustomScanState *node)
{
	return ExecScan(&node->ss, (ExecScanAccessMtd) lancescan_next,
					(ExecScanRecheckMtd) lancescan_recheck);
}

static void
end_reader(LanceScanState *s)
{
	if (s->reader)
	{
		s->batches += s->reader->batches_read;
		lt_reader_end(s->reader);
	}
	s->reader = NULL;
}

static void
lancescan_end(CustomScanState *node)
{
	LanceScanState *s = (LanceScanState *) node;

	end_reader(s);
	if (s->pscan)
		table_endscan(s->pscan);
	s->pscan = NULL;
}

/* A rescan starts a scan anew, its values evaluated again */
static void
lancescan_rescan(CustomScanState *node)
{
	LanceScanState *s = (LanceScanState *) node;

	end_reader(s);
	s->started = false;
	ExecScanReScan(&node->ss);
}

/*
 * A parallel scan: the access method's parallel scan state in the DSM -- the
 * snapshot, and the counter of the version's fragments taken (tableam.c) --
 * and in each participant the access method's scan of it, from which the
 * participant takes a reader of LanceScan's columns and filter for each
 * fragment it takes.  A parallel-aware plan run without the DSM, as a
 * Gather run by no workers may be, scans the whole table.
 */
static uint32
parallel_scan_flags(CustomScanState *node)
{
	return ScanRelIsReadOnly(&node->ss) ? SO_HINT_REL_READ_ONLY : SO_NONE;
}

static Size
lancescan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	return table_parallelscan_estimate(node->ss.ss_currentRelation,
									   node->ss.ps.state->es_snapshot);
}

static void
lancescan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate)
{
	LanceScanState *s = (LanceScanState *) node;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	table_parallelscan_initialize(node->ss.ss_currentRelation, pscan,
								  node->ss.ps.state->es_snapshot);
	s->pscan = table_beginscan_parallel(node->ss.ss_currentRelation, pscan,
										parallel_scan_flags(node));
}

static void
lancescan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *coordinate)
{
	table_parallelscan_reinitialize(node->ss.ss_currentRelation,
									(ParallelTableScanDesc) coordinate);
}

static void
lancescan_initialize_worker(CustomScanState *node, shm_toc *toc, void *coordinate)
{
	LanceScanState *s = (LanceScanState *) node;

	s->pscan = table_beginscan_parallel(node->ss.ss_currentRelation,
										(ParallelTableScanDesc) coordinate,
										parallel_scan_flags(node));
}

static const char *
metric_name(int metric)
{
	switch (metric)
	{
		case LE_METRIC_L2:
			return "l2";
		case LE_METRIC_COSINE:
			return "cosine";
		case LE_METRIC_DOT:
			return "dot";
		case LE_METRIC_HAMMING:
			return "hamming";
		default:
			return "default";
	}
}

/* EXPLAIN's: a value as the expression it is evaluated from */
typedef struct ExplainValues
{
	List	   *exprs;
	List	   *context;
	bool		useprefix;
} ExplainValues;

static bool
append_explain_value(StringInfo buf, int value, void *arg)
{
	ExplainValues *ev = (ExplainValues *) arg;
	Node	   *expr = list_nth(ev->exprs, value);

	/* an IN list's ARRAY[...], as the list */
	if (IsA(expr, ArrayExpr) && !((ArrayExpr *) expr)->multidims)
	{
		ListCell   *lc;

		foreach(lc, ((ArrayExpr *) expr)->elements)
			appendStringInfo(buf, "%s%s", foreach_current_index(lc) > 0 ? ", " : "",
							 deparse_expression(lfirst(lc), ev->context, ev->useprefix, false));
		return true;
	}
	appendStringInfoString(buf, deparse_expression(expr, ev->context, ev->useprefix, false));
	return true;
}

static void
lancescan_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	LanceScanState *s = (LanceScanState *) node;
	Relation	rel = node->ss.ss_currentRelation;
	TupleDesc	desc = RelationGetDescr(rel);
	ListCell   *lc;

	if (s->attnums == NIL)
		ExplainPropertyText("Lance Columns", "all", es);
	else
	{
		StringInfoData buf;
		bool		first = true;

		initStringInfo(&buf);
		foreach(lc, s->attnums)
		{
			Form_pg_attribute att = TupleDescAttr(desc, lfirst_int(lc) - 1);

			appendStringInfo(&buf, "%s%s", first ? "" : ", ", NameStr(att->attname));
			first = false;
		}
		ExplainPropertyText("Lance Columns", buf.data, es);
	}
	if (s->filter || s->rt_pieces)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		if (s->filter)
			appendStringInfoString(&buf, s->filter);
		if (s->rt_pieces)
		{
			CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
			ExplainValues ev;

			ev.exprs = list_copy_tail(cscan->custom_exprs,
									  list_length(cscan->custom_exprs) - s->rt_nvalues);
			ev.context = set_deparse_context_plan(es->deparse_cxt, node->ss.ps.plan, ancestors);
			ev.useprefix = list_length(es->rtable) > 1 || es->verbose;
			foreach(lc, s->rt_pieces)
			{
				if (buf.len > 0)
					appendStringInfoString(&buf, " AND ");
				(void) lt_filter_pieces_append(&buf, lfirst(lc), append_explain_value, &ev);
			}
		}
		ExplainPropertyText("Lance Filter", buf.data, es);
	}
	if (s->indices)
	{
		StringInfoData buf;

		initStringInfo(&buf);
		foreach(lc, s->indices)
			appendStringInfo(&buf, "%s%s", buf.len > 0 ? ", " : "", strVal(lfirst(lc)));
		ExplainPropertyText("Lance Index", buf.data, es);
	}
	if (s->nn_attnum > 0)
	{
		Form_pg_attribute att = TupleDescAttr(desc, s->nn_attnum - 1);
		char	   *text = psprintf("%s %s $query, k=%d, metric=%s, %s", NameStr(att->attname),
									s->nn_opname, s->nn_k, metric_name(s->nn_metric),
									s->nn_prefilter ? "prefilter" : "postfilter");

		ExplainPropertyText("Lance Search", text, es);
	}
	if (es->analyze && s->reader)
		ExplainPropertyInteger("Lance Version", NULL, (int64) s->reader->version, es);
	if (es->analyze && s->scans > 0)
	{
		if (s->scans > 1)
			ExplainPropertyInteger("Lance Scans", NULL, (int64) s->scans, es);
		ExplainPropertyInteger("Lance Batches", NULL,
							   (int64) (s->batches + (s->reader ? s->reader->batches_read : 0)),
							   es);
	}
}

/*
 * No parallel workers for a table the transaction has written: a parallel
 * CREATE INDEX plans its workers from this, as a query's partial paths do.
 */
static void
lt_build_simple_rel(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	LtXactTable *xt;

	if (prev_build_simple_rel)
		prev_build_simple_rel(root, rel, rte);
	if (rte->rtekind != RTE_RELATION)
		return;
	xt = lt_xact_table_by_relid(rte->relid);
	if (xt && xt->write)
		rel->rel_parallel_workers = 0;
}

void
lt_customscan_init(void)
{
	RegisterCustomScanMethods(&lancescan_scan_methods);
	prev_set_rel_pathlist = set_rel_pathlist_hook;
	set_rel_pathlist_hook = lt_set_rel_pathlist;
	prev_build_simple_rel = build_simple_rel_hook;
	build_simple_rel_hook = lt_build_simple_rel;
}
