/*-------------------------------------------------------------------------
 *
 * filter.c
 *	  A scan's quals as a Lance SQL predicate (docs/design.md §2.8): what
 *	  Lance's scanner filters by, with its scalar indices where the dataset
 *	  has them, before PostgreSQL sees a row.
 *
 *	  Only what Lance decides as PostgreSQL does is given: comparisons of a
 *	  column with a constant on integer, boolean, date and text types (text
 *	  ranges under the C collation alone, equality under any deterministic
 *	  one, as byte equality is equality there), IN lists of those, IS
 *	  [NOT] NULL, and AND, OR and NOT of them.  Floating-point comparisons
 *	  stay PostgreSQL's: -0 and NaN compare otherwise in Arrow.  Every qual
 *	  is evaluated again by the executor, so a predicate given only narrows
 *	  what is read.
 *
 *	  LanceScan's quals (lt_filter_clause) may compare a column with a value
 *	  the scan is given when it starts rather than a constant: a parameter
 *	  of a generic plan, the outer row's value of a nested loop, an InitPlan's
 *	  result, a stable expression of them.  Such a qual is deparsed when the
 *	  plan is made into pieces, the values left as their numbers, and its
 *	  predicate is made when the scan starts, of the values it is given then
 *	  (lt_filter_pieces_append); a value Lance cannot take as a literal of
 *	  the column leaves that qual PostgreSQL's alone.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/pg_locale.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"

#include "lance_tam.h"

#define MAX_IN_LIST		1000

typedef struct FilterContext
{
	Relation	rel;
	LtTable    *table;
	Index		varno;
	/* a LanceScan qual's (lt_filter_clause), else zeros */
	PlannerInfo *root;
	bool		runtime;		/* values other than constants are taken */
	Relids		outer_relids;	/* the relations whose Vars a value may hold */
	List	   *pieces;
	List	   *values;
	List	   *value_info;
	int			next_value;
	int			or_depth;		/* ORs above the node, an AND under NOT one
								 * too: a NULL there leaves the qual
								 * undecided, not false */
	bool		negated;		/* a NOT above the node */
	Bitmapset  *attnums;
	bool		sargable;
	int			nsearch;
} FilterContext;

/* A column, quoted as Lance's filters quote one: in backticks */
static void
append_ident(StringInfo buf, const char *name)
{
	appendStringInfoChar(buf, '`');
	for (const char *p = name; *p; p++)
	{
		if (*p == '`')
			appendStringInfoChar(buf, '`');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '`');
}

static void
append_string_literal(StringInfo buf, const char *s)
{
	appendStringInfoChar(buf, '\'');
	for (const char *p = s; *p; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
}

typedef enum ColClass
{
	COL_NONE,
	COL_INT,
	COL_BOOL,
	COL_DATE,
	COL_TEXT
} ColClass;

static ColClass
type_class(Oid typid)
{
	switch (typid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return COL_INT;
		case BOOLOID:
			return COL_BOOL;
		case DATEOID:
			return COL_DATE;
		case TEXTOID:
		case VARCHAROID:
			return COL_TEXT;
		default:
			return COL_NONE;
	}
}

/* The Lance column a Var of the scanned relation reads, or NULL */
static const char *
var_column(FilterContext *cxt, Node *node, Oid *typid, Oid *collid)
{
	Var		   *var;

	if (node && IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;
	if (node == NULL || !IsA(node, Var))
		return NULL;
	var = (Var *) node;
	if (var->varno != cxt->varno || var->varattno <= 0 || var->varlevelsup != 0 ||
		var->varattno > cxt->table->natts)
		return NULL;
	if (cxt->table->columns[var->varattno - 1] == NULL)
		return NULL;
	*typid = getBaseType(var->vartype);
	*collid = var->varcollid;
	cxt->attnums = bms_add_member(cxt->attnums, var->varattno);
	return cxt->table->columns[var->varattno - 1];
}

/* Whether a constant of the type is a literal of a column of the class */
static bool
const_type_ok(ColClass cls, Oid typid)
{
	switch (cls)
	{
		case COL_INT:
			return typid == INT2OID || typid == INT4OID || typid == INT8OID;
		case COL_BOOL:
			return typid == BOOLOID;
		case COL_DATE:
			return typid == DATEOID;
		case COL_TEXT:
			return (typid == TEXTOID || typid == VARCHAROID) &&
				(GetDatabaseEncoding() == PG_UTF8 || GetDatabaseEncoding() == PG_SQL_ASCII);
		default:
			return false;
	}
}

/*
 * A constant as a literal of the column's class, or false.  An integer
 * outside the column's type -- int2col < 100000, by a cross-type operator
 * -- is no literal of that type to Lance, and stays PostgreSQL's to test.
 */
static bool
append_const(StringInfo buf, ColClass cls, Oid coltype, Oid typid, Datum value)
{
	switch (cls)
	{
		case COL_INT:
			{
				int64		v;

				switch (typid)
				{
					case INT2OID:
						v = DatumGetInt16(value);
						break;
					case INT4OID:
						v = DatumGetInt32(value);
						break;
					case INT8OID:
						v = DatumGetInt64(value);
						break;
					default:
						return false;
				}
				if ((coltype == INT2OID && (v < PG_INT16_MIN || v > PG_INT16_MAX)) ||
					(coltype == INT4OID && (v < PG_INT32_MIN || v > PG_INT32_MAX)))
					return false;
				appendStringInfo(buf, INT64_FORMAT, v);
				return true;
			}
		case COL_BOOL:
			if (typid != BOOLOID)
				return false;
			appendStringInfoString(buf, DatumGetBool(value) ? "true" : "false");
			return true;
		case COL_DATE:
			{
				DateADT		d;
				int			year;
				int			month;
				int			day;

				if (typid != DATEOID)
					return false;
				d = DatumGetDateADT(value);
				if (DATE_NOT_FINITE(d))
					return false;
				j2date(d + POSTGRES_EPOCH_JDATE, &year, &month, &day);
				if (year <= 0 || year > 9999)
					return false;
				appendStringInfo(buf, "DATE '%04d-%02d-%02d'", year, month, day);
				return true;
			}
		case COL_TEXT:
			if (typid != TEXTOID && typid != VARCHAROID)
				return false;
			{
				char	   *s = TextDatumGetCString(value);

				if (GetDatabaseEncoding() != PG_UTF8 && GetDatabaseEncoding() != PG_SQL_ASCII)
					return false;
				append_string_literal(buf, s);
				return true;
			}
		default:
			return false;
	}
}

/*
 * A value a LanceScan qual compares a column with that the scan is given
 * when it starts -- an expression of parameters, of the Vars of the
 * relations the scan is parameterized by and of stable functions, never of
 * the scanned relation's -- in the predicate as its number.  An array's are
 * the values of an IN list.
 */
static bool
append_value(FilterContext *cxt, StringInfo buf, Node *value, ColClass cls, Oid coltype,
			 bool array)
{
	Relids		varnos;
	Oid			valtype;

	if (!cxt->runtime || contain_volatile_functions(value) || contain_subplans(value))
		return false;
	varnos = pull_varnos(cxt->root, value);
	if (bms_is_member(cxt->varno, varnos) || !bms_is_subset(varnos, cxt->outer_relids))
		return false;
	valtype = getBaseType(exprType(value));
	if (array)
	{
		valtype = get_element_type(valtype);
		if (!OidIsValid(valtype))
			return false;
		valtype = getBaseType(valtype);
	}
	if (!const_type_ok(cls, valtype))
		return false;
	cxt->pieces = lappend(cxt->pieces, makeString(pstrdup(buf->data)));
	resetStringInfo(buf);
	cxt->pieces = lappend(cxt->pieces, makeInteger(cxt->next_value++));
	cxt->values = lappend(cxt->values, value);
	cxt->value_info = lappend(cxt->value_info,
							  lappend_int(list_make5_int(cls, coltype, valtype, array,
														 cxt->or_depth == 0),
										  !cxt->negated));
	return true;
}

static const char *
comparison(Oid opno, bool *is_equality, bool *commuted_ok)
{
	char	   *name = get_opname(opno);

	*is_equality = false;
	*commuted_ok = true;
	if (name == NULL)
		return NULL;
	if (strcmp(name, "=") == 0)
	{
		*is_equality = true;
		return "=";
	}
	if (strcmp(name, "<>") == 0)
	{
		*is_equality = true;
		return "<>";
	}
	if (strcmp(name, "<") == 0)
		return "<";
	if (strcmp(name, "<=") == 0)
		return "<=";
	if (strcmp(name, ">") == 0)
		return ">";
	if (strcmp(name, ">=") == 0)
		return ">=";
	return NULL;
}

static const char *
commute(const char *op)
{
	if (strcmp(op, "<") == 0)
		return ">";
	if (strcmp(op, "<=") == 0)
		return ">=";
	if (strcmp(op, ">") == 0)
		return "<";
	if (strcmp(op, ">=") == 0)
		return "<=";
	return op;
}

/* The operator must be the btree comparison of its types, not a lookalike */
static bool
builtin_comparison(Oid opno)
{
	return get_op_rettype(opno) == BOOLOID && opno < FirstNormalObjectId;
}

static bool
text_op_ok(Oid collid, bool is_equality)
{
	pg_locale_t locale;

	if (!OidIsValid(collid))
		return false;
	locale = pg_newlocale_from_collation(collid);
	if (locale->collate_is_c)
		return true;
	return is_equality && locale->deterministic;
}

static bool deparse(FilterContext *cxt, Node *node, StringInfo buf);

static bool
deparse_opexpr(FilterContext *cxt, OpExpr *op, StringInfo buf)
{
	Node	   *left;
	Node	   *right;
	const char *column;
	const char *sym;
	bool		is_equality;
	bool		commuted_ok;
	bool		commuted = false;
	Oid			typid;
	Oid			collid;
	Const	   *c;
	ColClass	cls;

	if (list_length(op->args) != 2 || !builtin_comparison(op->opno))
		return false;
	sym = comparison(op->opno, &is_equality, &commuted_ok);
	if (sym == NULL)
		return false;
	left = linitial(op->args);
	right = lsecond(op->args);
	column = var_column(cxt, left, &typid, &collid);
	if (column == NULL)
	{
		column = var_column(cxt, right, &typid, &collid);
		if (column == NULL)
			return false;
		right = left;
		commuted = true;
	}
	if (right && IsA(right, RelabelType))
		right = (Node *) ((RelabelType *) right)->arg;
	if (right == NULL || (IsA(right, Const) && ((Const *) right)->constisnull))
		return false;
	cls = type_class(typid);
	if (cls == COL_NONE)
		return false;
	if (cls == COL_TEXT && !text_op_ok(op->inputcollid, is_equality))
		return false;
	if (commuted)
		sym = commute(sym);
	appendStringInfoChar(buf, '(');
	append_ident(buf, column);
	appendStringInfo(buf, " %s ", sym);
	if (IsA(right, Const))
	{
		c = (Const *) right;
		if (!append_const(buf, cls, typid, getBaseType(c->consttype), c->constvalue))
			return false;
	}
	else if (!append_value(cxt, buf, right, cls, typid, false))
		return false;
	appendStringInfoChar(buf, ')');
	/* <> is the index's equality negated: most rows, read through it */
	if (strcmp(sym, "<>") == 0)
		cxt->sargable = false;
	cxt->nsearch++;
	return true;
}

static bool
deparse_in_list(FilterContext *cxt, ScalarArrayOpExpr *saop, StringInfo buf)
{
	const char *column;
	Oid			typid;
	Oid			collid;
	Node	   *arr;
	Const	   *c;
	ArrayType  *a;
	Oid			elemtype;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *elems;
	bool	   *nulls;
	int			n;
	bool		is_equality;
	bool		commuted_ok;
	const char *sym;
	ColClass	cls;
	int			k = 0;

	if (!saop->useOr || list_length(saop->args) != 2 || !builtin_comparison(saop->opno))
		return false;
	sym = comparison(saop->opno, &is_equality, &commuted_ok);
	if (sym == NULL || strcmp(sym, "=") != 0)
		return false;
	column = var_column(cxt, linitial(saop->args), &typid, &collid);
	if (column == NULL)
		return false;
	cls = type_class(typid);
	if (cls == COL_NONE || (cls == COL_TEXT && !text_op_ok(saop->inputcollid, true)))
		return false;
	arr = lsecond(saop->args);
	if (!IsA(arr, Const))
	{
		appendStringInfoChar(buf, '(');
		append_ident(buf, column);
		appendStringInfoString(buf, " IN (");
		if (!append_value(cxt, buf, arr, cls, typid, true))
			return false;
		appendStringInfoString(buf, "))");
		cxt->nsearch += cxt->root ?
			(int) Min(estimate_array_length(cxt->root, arr), MAX_IN_LIST) : 10;
		return true;
	}
	if (((Const *) arr)->constisnull)
		return false;
	c = (Const *) arr;
	a = DatumGetArrayTypeP(c->constvalue);
	elemtype = ARR_ELEMTYPE(a);
	get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);
	deconstruct_array(a, elemtype, elmlen, elmbyval, elmalign, &elems, &nulls, &n);
	if (n == 0 || n > MAX_IN_LIST)
		return false;
	appendStringInfoChar(buf, '(');
	append_ident(buf, column);
	appendStringInfoString(buf, " IN (");
	for (int i = 0; i < n; i++)
	{
		if (nulls[i])
			continue;
		if (k++ > 0)
			appendStringInfoString(buf, ", ");
		if (!append_const(buf, cls, typid, getBaseType(elemtype), elems[i]))
			return false;
	}
	/* a list of nulls only: no literal for Lance */
	if (k == 0)
		return false;
	appendStringInfoString(buf, "))");
	cxt->nsearch += k;
	return true;
}

static bool
deparse(FilterContext *cxt, Node *node, StringInfo buf)
{
	if (node == NULL)
		return false;
	switch (nodeTag(node))
	{
		case T_OpExpr:
			return deparse_opexpr(cxt, (OpExpr *) node, buf);
		case T_ScalarArrayOpExpr:
			return deparse_in_list(cxt, (ScalarArrayOpExpr *) node, buf);
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;
				Oid			typid;
				Oid			collid;
				const char *column;

				if (nt->argisrow)
					return false;
				column = var_column(cxt, (Node *) nt->arg, &typid, &collid);
				if (column == NULL)
					return false;
				appendStringInfoChar(buf, '(');
				append_ident(buf, column);
				appendStringInfoString(buf, nt->nulltesttype == IS_NULL ? " IS NULL)" : " IS NOT NULL)");
				if (nt->nulltesttype != IS_NULL)
					cxt->sargable = false;
				cxt->nsearch++;
				return true;
			}
		case T_Var:
			{
				Oid			typid;
				Oid			collid;
				const char *column = var_column(cxt, node, &typid, &collid);

				if (column == NULL || typid != BOOLOID)
					return false;
				appendStringInfoChar(buf, '(');
				append_ident(buf, column);
				appendStringInfoString(buf, " = true)");
				cxt->nsearch++;
				return true;
			}
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;
				ListCell   *lc;
				bool		first = true;
				bool		loose;

				if (b->boolop == NOT_EXPR)
				{
					appendStringInfoString(buf, "(NOT ");
					cxt->negated = !cxt->negated;
					if (!deparse(cxt, linitial(b->args), buf))
						return false;
					cxt->negated = !cxt->negated;
					appendStringInfoChar(buf, ')');
					cxt->sargable = false;
					return true;
				}
				/* NOT (a AND b) is NOT a OR NOT b */
				loose = (b->boolop == OR_EXPR) != cxt->negated;
				appendStringInfoChar(buf, '(');
				if (loose)
					cxt->or_depth++;
				foreach(lc, b->args)
				{
					if (!first)
						appendStringInfoString(buf, b->boolop == AND_EXPR ? " AND " : " OR ");
					first = false;
					if (!deparse(cxt, lfirst(lc), buf))
						return false;
				}
				if (loose)
					cxt->or_depth--;
				appendStringInfoChar(buf, ')');
				return true;
			}
		default:
			return false;
	}
}

/*
 * The quals (RestrictInfos or bare clauses) Lance can decide, ANDed, as a
 * predicate, or NULL; *pushed gets the clauses given.
 */
char *
lt_filter_from_quals(Relation rel, LtTable *t, List *quals, List **pushed)
{
	FilterContext cxt;
	StringInfoData out;
	ListCell   *lc;
	int			n = 0;

	if (pushed)
		*pushed = NIL;
	if (!lt_enable_filter_pushdown || quals == NIL)
		return NULL;
	memset(&cxt, 0, sizeof(cxt));
	cxt.rel = rel;
	cxt.table = t;
	cxt.varno = 0;
	initStringInfo(&out);
	foreach(lc, quals)
	{
		Node	   *clause = lfirst(lc);
		StringInfoData one;

		if (IsA(clause, RestrictInfo))
		{
			RestrictInfo *ri = (RestrictInfo *) clause;

			if (ri->pseudoconstant || bms_membership(ri->clause_relids) != BMS_SINGLETON)
				continue;
			cxt.varno = bms_singleton_member(ri->clause_relids);
			clause = (Node *) ri->clause;
		}
		else
		{
			Relids		varnos = pull_varnos(NULL, clause);

			if (bms_membership(varnos) != BMS_SINGLETON)
				continue;
			cxt.varno = bms_singleton_member(varnos);
		}
		if (contain_volatile_functions(clause))
			continue;
		initStringInfo(&one);
		if (!deparse(&cxt, clause, &one))
			continue;
		if (n++ > 0)
			appendStringInfoString(&out, " AND ");
		appendStringInfoString(&out, one.data);
		if (pushed)
			*pushed = lappend(*pushed, lfirst(lc));
	}
	return n > 0 ? out.data : NULL;
}

/*
 * A qual of LanceScan's (a RestrictInfo or a bare clause of the relation
 * varno) as Lance decides it, or NULL: its values numbered from
 * first_value, a Var of outer_relids among them.
 */
LtFilterClause *
lt_filter_clause(PlannerInfo *root, Relation rel, LtTable *t, Index varno, Node *clause,
				 Relids outer_relids, int first_value)
{
	FilterContext cxt;
	StringInfoData buf;
	LtFilterClause *fc;

	if (!lt_enable_filter_pushdown)
		return NULL;
	if (IsA(clause, RestrictInfo))
	{
		if (((RestrictInfo *) clause)->pseudoconstant)
			return NULL;
		clause = (Node *) ((RestrictInfo *) clause)->clause;
	}
	if (contain_volatile_functions(clause))
		return NULL;
	memset(&cxt, 0, sizeof(cxt));
	cxt.rel = rel;
	cxt.table = t;
	cxt.varno = varno;
	cxt.root = root;
	cxt.runtime = true;
	cxt.outer_relids = outer_relids;
	cxt.next_value = first_value;
	cxt.sargable = true;
	initStringInfo(&buf);
	if (!deparse(&cxt, clause, &buf))
		return NULL;
	if (buf.len > 0)
		cxt.pieces = lappend(cxt.pieces, makeString(buf.data));
	fc = palloc0(sizeof(LtFilterClause));
	fc->pieces = cxt.pieces;
	fc->values = cxt.values;
	fc->value_info = cxt.value_info;
	fc->attnums = cxt.attnums;
	fc->sargable = cxt.sargable;
	fc->nsearch = Max(cxt.nsearch, 1);
	return fc;
}

/* A qual's predicate from its pieces, fn appending each value; false when it fails */
bool
lt_filter_pieces_append(StringInfo buf, List *pieces, LtFilterValueFn fn, void *arg)
{
	ListCell   *lc;

	foreach(lc, pieces)
	{
		Node	   *piece = lfirst(lc);

		if (IsA(piece, String))
			appendStringInfoString(buf, strVal(piece));
		else if (!fn(buf, intVal(piece), arg))
			return false;
	}
	return true;
}

/*
 * A value the scan was given, as the literal (or an IN list's literals) of
 * its column, info its lt_filter_clause()'s: false when Lance cannot take
 * it.  *never is set when the qual is false or NULL for every row whatever
 * the rest of it: a NULL value (or a list of nulls) with no OR above it,
 * nor an AND under a NOT, since NOT NULL is NULL; an empty list, whose
 * comparison is false, with no OR and no NOT above it.
 */
bool
lt_filter_value_append(StringInfo buf, List *info, Datum value, bool isnull, bool *never)
{
	ColClass	cls = (ColClass) linitial_int(info);
	Oid			coltype = (Oid) lsecond_int(info);
	Oid			valtype = (Oid) lthird_int(info);
	bool		array = lfourth_int(info) != 0;
	bool		strict = list_nth_int(info, 4) != 0;
	bool		positive = list_nth_int(info, 5) != 0;
	ArrayType  *a;
	int16		elmlen;
	bool		elmbyval;
	char		elmalign;
	Datum	   *elems;
	bool	   *nulls;
	int			n;
	int			k = 0;

	if (isnull)
	{
		if (strict)
			*never = true;
		return false;
	}
	if (!array)
		return append_const(buf, cls, coltype, valtype, value);
	a = DatumGetArrayTypeP(value);
	get_typlenbyvalalign(ARR_ELEMTYPE(a), &elmlen, &elmbyval, &elmalign);
	deconstruct_array(a, ARR_ELEMTYPE(a), elmlen, elmbyval, elmalign, &elems, &nulls, &n);
	if (n > MAX_IN_LIST)
		return false;
	for (int i = 0; i < n; i++)
	{
		if (nulls[i])
			continue;
		if (k++ > 0)
			appendStringInfoString(buf, ", ");
		if (!append_const(buf, cls, coltype, valtype, elems[i]))
			return false;
	}
	if (k == 0)
	{
		/* = ANY of nulls is NULL; of no value, false */
		if (strict && (n > 0 || positive))
			*never = true;
		return false;
	}
	return true;
}
