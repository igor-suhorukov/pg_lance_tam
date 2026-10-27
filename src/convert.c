/*-------------------------------------------------------------------------
 *
 * convert.c
 *	  A relation's attributes as Arrow columns (docs/design.md §2.5): the
 *	  layout the relation's types map to, the schemas exported for a
 *	  dataset and for a scan, the batches a write forms from slots, and the
 *	  values of the batches a scan reads.
 *
 *		PostgreSQL					Arrow (the dataset's column)
 *		boolean						bool
 *		smallint, integer, bigint	int16, int32, int64
 *		real, double precision		float32, float64
 *		numeric(p, s), p <= 38		decimal128(p, s)
 *		text, varchar, char			utf8
 *		bytea						binary
 *		date						date32, Arrow's epoch
 *		time						time64[us]
 *		timestamp, timestamptz		timestamp[us], without or with UTC
 *		uuid						fixed_size_binary(16)
 *		vector(n), halfvec(n)		fixed_size_list<float32 | float16, n>
 *									(pgvector's types)
 *		a one-dimensional array		list<element> of a type above
 *		any other type				utf8, its text form
 *
 *	  The batches a write exports are malloc'd, since the library may hold
 *	  them after the call returns: their release callbacks free them on any
 *	  thread and touch nothing of PostgreSQL's.  The batches a scan reads
 *	  come in the types the scan asked for, cast by the library, so a column
 *	  is read by its kind alone.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "mb/pg_wchar.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
#include "varatt.h"

#include "lance_tam.h"

/* Days and microseconds from Arrow's epoch, 1970-01-01, to PostgreSQL's */
#define DAYS_1970_TO_2000		10957
#define USECS_1970_TO_2000		INT64CONST(946684800000000)

/* pgvector's on-disk layouts (pgvector's src/vector.h, src/halfvec.h) */
typedef struct PgVector
{
	int32		vl_len_;
	int16		dim;
	int16		unused;
	float4		x[FLEXIBLE_ARRAY_MEMBER];
} PgVector;

typedef struct PgHalfVec
{
	int32		vl_len_;
	int16		dim;
	int16		unused;
	uint16		x[FLEXIBLE_ARRAY_MEMBER];
} PgHalfVec;

/* ------------------------------------------------------------------------
 * The layout of a relation
 * ------------------------------------------------------------------------
 */

const char *
lt_kind_name(LtKind kind)
{
	switch (kind)
	{
		case LT_NONE:
			return "none";
		case LT_BOOL:
			return "bool";
		case LT_INT2:
			return "int16";
		case LT_INT4:
			return "int32";
		case LT_INT8:
			return "int64";
		case LT_FLOAT4:
			return "float32";
		case LT_FLOAT8:
			return "float64";
		case LT_NUMERIC:
			return "decimal128";
		case LT_TEXT:
			return "utf8";
		case LT_BYTEA:
			return "binary";
		case LT_DATE:
			return "date32";
		case LT_TIME:
			return "time64[us]";
		case LT_TIMESTAMP:
			return "timestamp[us]";
		case LT_TIMESTAMPTZ:
			return "timestamp[us, UTC]";
		case LT_UUID:
			return "fixed_size_binary(16)";
		case LT_VECTOR:
			return "fixed_size_list<float32>";
		case LT_HALFVEC:
			return "fixed_size_list<float16>";
		case LT_ARRAY:
			return "list";
		case LT_IO:
			return "utf8 (text form)";
	}
	return "?";
}

/*
 * Whether a type is pgvector's vector or halfvec: by its name and its input
 * function's, since pgvector may be installed in any schema, or not at all.
 */
bool
lt_type_is_vector(Oid typid, int *dim, bool *half)
{
	HeapTuple	tup;
	Form_pg_type typ;
	bool		result = false;

	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (!HeapTupleIsValid(tup))
		return false;
	typ = (Form_pg_type) GETSTRUCT(tup);
	if (typ->typlen == -1 &&
		(strcmp(NameStr(typ->typname), "vector") == 0 ||
		 strcmp(NameStr(typ->typname), "halfvec") == 0))
	{
		char	   *fn = get_func_name(typ->typinput);

		if (fn && (strcmp(fn, "vector_in") == 0 || strcmp(fn, "halfvec_in") == 0))
		{
			result = true;
			if (half)
				*half = strcmp(fn, "halfvec_in") == 0;
		}
	}
	ReleaseSysCache(tup);
	if (dim)
		*dim = 0;
	return result;
}

/* The kind of a scalar type, or LT_IO; fills the column's Arrow format */
static LtKind
scalar_kind(Oid typid, int32 typmod, LtColumn *col, char *format, Size fmtlen)
{
	switch (typid)
	{
		case BOOLOID:
			strlcpy(format, "b", fmtlen);
			return LT_BOOL;
		case INT2OID:
			strlcpy(format, "s", fmtlen);
			return LT_INT2;
		case INT4OID:
			strlcpy(format, "i", fmtlen);
			return LT_INT4;
		case INT8OID:
			strlcpy(format, "l", fmtlen);
			return LT_INT8;
		case FLOAT4OID:
			strlcpy(format, "f", fmtlen);
			return LT_FLOAT4;
		case FLOAT8OID:
			strlcpy(format, "g", fmtlen);
			return LT_FLOAT8;
		case NUMERICOID:
			if (typmod >= (int32) VARHDRSZ)
			{
				int32		precision = ((typmod - VARHDRSZ) >> 16) & 0xffff;
				int32		scale = (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;

				if (precision >= 1 && precision <= 38 && scale >= 0 && scale <= precision)
				{
					if (col)
					{
						col->precision = precision;
						col->scale = scale;
					}
					snprintf(format, fmtlen, "d:%d,%d", precision, scale);
					return LT_NUMERIC;
				}
			}
			break;
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			strlcpy(format, "u", fmtlen);
			return LT_TEXT;
		case BYTEAOID:
			strlcpy(format, "z", fmtlen);
			return LT_BYTEA;
		case DATEOID:
			strlcpy(format, "tdD", fmtlen);
			return LT_DATE;
		case TIMEOID:
			strlcpy(format, "ttu", fmtlen);
			return LT_TIME;
		case TIMESTAMPOID:
			strlcpy(format, "tsu:", fmtlen);
			return LT_TIMESTAMP;
		case TIMESTAMPTZOID:
			strlcpy(format, "tsu:UTC", fmtlen);
			return LT_TIMESTAMPTZ;
		case UUIDOID:
			strlcpy(format, "w:16", fmtlen);
			return LT_UUID;
		default:
			break;
	}
	strlcpy(format, "u", fmtlen);
	return LT_IO;
}

static void
set_io(LtColumn *col, Oid typid, MemoryContext cxt)
{
	Oid			out;
	Oid			in;
	bool		varlena;

	getTypeOutputInfo(typid, &out, &varlena);
	getTypeInputInfo(typid, &in, &col->ioparam);
	fmgr_info_cxt(out, &col->out_fn, cxt);
	fmgr_info_cxt(in, &col->in_fn, cxt);
}

static void
build_column(LtColumn *col, Form_pg_attribute att, const char *name, MemoryContext cxt)
{
	int32		typmod = att->atttypmod;
	Oid			typid = getBaseTypeAndTypmod(att->atttypid, &typmod);
	bool		half = false;
	Oid			elem;

	memset(col, 0, sizeof(*col));
	col->attnum = att->attnum;
	col->name = MemoryContextStrdup(cxt, name);
	col->typid = typid;
	col->typmod = typmod;

	if (lt_type_is_vector(typid, NULL, &half))
	{
		if (typmod <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" of a lance table must declare its vector's dimension",
							NameStr(att->attname)),
					 errhint("Declare it as %s(n).", half ? "halfvec" : "vector")));
		col->kind = half ? LT_HALFVEC : LT_VECTOR;
		col->dim = typmod;
		snprintf(col->format, sizeof(col->format), "+w:%d", typmod);
		strlcpy(col->child_format, half ? "e" : "f", sizeof(col->child_format));
		return;
	}

	elem = get_element_type(typid);
	if (OidIsValid(elem) && get_typlen(typid) == -1)
	{
		LtColumn	ecol;
		char		efmt[24];
		LtKind		ek;
		int32		etypmod = typmod;
		Oid			ebase = getBaseTypeAndTypmod(elem, &etypmod);

		memset(&ecol, 0, sizeof(ecol));
		ek = scalar_kind(ebase, etypmod, &ecol, efmt, sizeof(efmt));
		if (ek != LT_IO)
		{
			col->kind = LT_ARRAY;
			col->elem_kind = ek;
			/* an array of a domain's values is one of the domain's type */
			col->elem_type = elem;
			col->precision = ecol.precision;
			col->scale = ecol.scale;
			get_typlenbyvalalign(ebase, &col->elem_len, &col->elem_byval, &col->elem_align);
			strlcpy(col->format, "+l", sizeof(col->format));
			strlcpy(col->child_format, efmt, sizeof(col->child_format));
			return;
		}
	}

	col->kind = scalar_kind(typid, typmod, col, col->format, sizeof(col->format));
	if (col->kind == LT_IO)
		set_io(col, att->atttypid, cxt);
}

/*
 * Whether two typings of an attribute -- before and after ALTER COLUMN
 * ... TYPE -- store the same Arrow values: the same type, its typmod's
 * precision, scale and dimension, and for a type kept as text the same
 * base type, whose input reads what its output wrote.
 */
bool
lt_attribute_maps_alike(Form_pg_attribute a, Form_pg_attribute b)
{
	LtColumn	ca;
	LtColumn	cb;

	build_column(&ca, a, "", CurrentMemoryContext);
	build_column(&cb, b, "", CurrentMemoryContext);
	if (ca.kind != cb.kind || strcmp(ca.format, cb.format) != 0 ||
		strcmp(ca.child_format, cb.child_format) != 0 || ca.elem_kind != cb.elem_kind ||
		ca.dim != cb.dim || ca.precision != cb.precision || ca.scale != cb.scale)
		return false;
	if (ca.kind == LT_IO || (ca.kind == LT_ARRAY && ca.elem_kind == LT_IO))
		return ca.typid == cb.typid;
	return true;
}

/*
 * A layout of a tuple descriptor, each attribute the Lance column names[i]
 * names (NULL: the attribute's own name; names NULL: every attribute's own),
 * built in a memory context of the caller's: a scan or a write keeps its
 * own for its life, which no invalidation ends.
 */
LtLayout *
lt_layout_build_desc(TupleDesc desc, char **names, int nnames, MemoryContext parent)
{
	MemoryContext cxt;
	LtLayout   *layout;

	cxt = AllocSetContextCreate(parent, "lance_tam layout", ALLOCSET_SMALL_SIZES);
	layout = MemoryContextAllocZero(cxt, sizeof(LtLayout));
	layout->cxt = cxt;
	layout->natts = desc->natts;
	layout->cols = MemoryContextAllocZero(cxt, sizeof(LtColumn) * Max(desc->natts, 1));
	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		const char *name;

		if (att->attisdropped)
			continue;
		if (names == NULL)
			name = att->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL ? NULL : NameStr(att->attname);
		else
			name = i < nnames ? names[i] : NULL;
		if (name == NULL)
		{
			/* an attribute the dataset has no column for: read as NULL */
			layout->cols[i].attnum = att->attnum;
			layout->cols[i].kind = LT_NONE;
			continue;
		}
		build_column(&layout->cols[i], att, name, cxt);
	}
	return layout;
}

LtLayout *
lt_layout_build(Relation rel, MemoryContext parent)
{
	LtTable    *t = lt_table(rel);

	return lt_layout_build_desc(RelationGetDescr(rel), t->columns, t->natts, parent);
}

LtLayout *
lt_layout(Relation rel)
{
	return lt_layout_build(rel, CurrentMemoryContext);
}

/* ------------------------------------------------------------------------
 * Exported schemas: malloc'd trees freed by their release callback
 * ------------------------------------------------------------------------
 */

typedef struct SchemaPrivate
{
	char	   *format;
	char	   *name;
	char	   *metadata;
	struct ArrowSchema **children;
	struct ArrowSchema *child_storage;
} SchemaPrivate;

static void
schema_release(struct ArrowSchema *schema)
{
	SchemaPrivate *p = (SchemaPrivate *) schema->private_data;

	if (schema->release == NULL)
		return;
	for (int64 i = 0; i < schema->n_children; i++)
	{
		struct ArrowSchema *child = schema->children[i];

		if (child->release)
			child->release(child);
	}
	if (p)
	{
		free(p->format);
		free(p->name);
		free(p->metadata);
		free(p->children);
		free(p->child_storage);
		free(p);
	}
	schema->release = NULL;
}

static char *
xstrdup(const char *s)
{
	char	   *d = strdup(s ? s : "");

	if (d == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	return d;
}

static void *
xcalloc(size_t n, size_t size)
{
	void	   *p = calloc(n ? n : 1, size ? size : 1);

	if (p == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	return p;
}

static void
schema_init(struct ArrowSchema *s, const char *format, const char *name, int64 nchildren,
			bool nullable)
{
	SchemaPrivate *p = xcalloc(1, sizeof(SchemaPrivate));

	p->format = xstrdup(format);
	p->name = xstrdup(name);
	if (nchildren > 0)
	{
		p->child_storage = xcalloc(nchildren, sizeof(struct ArrowSchema));
		p->children = xcalloc(nchildren, sizeof(struct ArrowSchema *));
		for (int64 i = 0; i < nchildren; i++)
			p->children[i] = &p->child_storage[i];
	}
	memset(s, 0, sizeof(*s));
	s->format = p->format;
	s->name = p->name;
	s->metadata = NULL;
	s->flags = nullable ? ARROW_FLAG_NULLABLE : 0;
	s->n_children = nchildren;
	s->children = p->children;
	s->dictionary = NULL;
	s->release = schema_release;
	s->private_data = p;
}

/*
 * A column's field, nullable whatever the attribute's NOT NULL: PostgreSQL
 * keeps the constraint, which ALTER TABLE sets and drops without the
 * dataset, and a dropped column's rows need no value.
 */
static void
column_schema(LtColumn *col, struct ArrowSchema *s)
{
	if (col->kind == LT_VECTOR || col->kind == LT_HALFVEC || col->kind == LT_ARRAY)
	{
		schema_init(s, col->format, col->name, 1, true);
		schema_init(s->children[0], col->child_format, "item", 0, true);
	}
	else
		schema_init(s, col->format, col->name, 0, true);
}

void
lt_export_schema(LtLayout *layout, int ncols, const int *attidx, struct ArrowSchema *out)
{
	schema_init(out, "+s", "", ncols, false);
	for (int i = 0; i < ncols; i++)
		column_schema(&layout->cols[attidx[i]], out->children[i]);
}

/*
 * A field of an exported schema given its metadata, in the Arrow C data
 * interface's form: copied, and freed with the schema.
 */
void
lt_schema_set_metadata(struct ArrowSchema *s, const char *metadata, size_t len)
{
	SchemaPrivate *p = (SchemaPrivate *) s->private_data;
	char	   *copy = malloc(len ? len : 1);

	if (copy == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	memcpy(copy, metadata, len);
	free(p->metadata);
	p->metadata = copy;
	s->metadata = copy;
}

/* ------------------------------------------------------------------------
 * Batches formed for a write
 * ------------------------------------------------------------------------
 */

/* A growing malloc'd buffer */
typedef struct Buf
{
	char	   *data;
	size_t		len;
	size_t		cap;
} Buf;

static void
buf_reserve(Buf *b, size_t extra)
{
	if (b->len + extra > b->cap)
	{
		size_t		cap = Max(b->cap * 2, Max(b->len + extra, (size_t) 1024));
		char	   *d = realloc(b->data, cap);

		if (d == NULL)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		/* the bytes past len are zeroed: bit buffers rely on it */
		memset(d + b->cap, 0, cap - b->cap);
		b->data = d;
		b->cap = cap;
	}
}

static void
buf_append(Buf *b, const void *p, size_t n)
{
	buf_reserve(b, n);
	if (n > 0)
		memcpy(b->data + b->len, p, n);
	b->len += n;
}

static void
buf_zero(Buf *b, size_t n)
{
	buf_reserve(b, n);
	memset(b->data + b->len, 0, n);
	b->len += n;
}

/* bit i of a bit buffer, LSB first, as Arrow's */
static void
bits_set(Buf *b, int64 i, bool v)
{
	size_t		byte = (size_t) (i >> 3);

	if (byte >= b->len)
	{
		buf_reserve(b, byte + 1 - b->len);
		b->len = byte + 1;
	}
	if (v)
		((uint8 *) b->data)[byte] |= (uint8) (1 << (i & 7));
	else
		((uint8 *) b->data)[byte] &= (uint8) ~(1 << (i & 7));
}

/* A column of a batch being formed: Arrow's buffers, then a child's */
typedef struct ColBuf
{
	Buf			validity;
	Buf			values;			/* fixed-width values, or bytes */
	Buf			offsets;		/* int32 offsets: n + 1 of them */
	int64		length;
	int64		null_count;
	struct ColBuf *child;
} ColBuf;

struct LtBuilder
{
	LtLayout   *layout;
	int			ncols;			/* attributes written */
	int		   *attidx;
	ColBuf	   *cols;
	Buf			rowids;
	int64		nrows;
	Size		bytes;
	bool		utf8;			/* the database's encoding is UTF8 */
	MemoryContext tmpcxt;
};

static void
colbuf_init(ColBuf *c, LtColumn *col)
{
	int32		zero = 0;

	memset(c, 0, sizeof(*c));
	if (col->kind == LT_TEXT || col->kind == LT_BYTEA || col->kind == LT_IO || col->kind == LT_ARRAY)
		buf_append(&c->offsets, &zero, sizeof(int32));
	if (col->kind == LT_VECTOR || col->kind == LT_HALFVEC || col->kind == LT_ARRAY)
	{
		c->child = calloc(1, sizeof(ColBuf));
		if (c->child == NULL)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		if (col->kind == LT_ARRAY &&
			(col->elem_kind == LT_TEXT || col->elem_kind == LT_BYTEA))
			buf_append(&c->child->offsets, &zero, sizeof(int32));
	}
}

static void
colbuf_free(ColBuf *c)
{
	if (c == NULL)
		return;
	free(c->validity.data);
	free(c->values.data);
	free(c->offsets.data);
	if (c->child)
	{
		colbuf_free(c->child);
		free(c->child);
	}
	memset(c, 0, sizeof(*c));
}

LtBuilder *
lt_builder_create(LtLayout *layout)
{
	LtBuilder  *b = palloc0(sizeof(LtBuilder));
	int			n = 0;

	b->layout = layout;
	b->attidx = palloc(sizeof(int) * Max(layout->natts, 1));
	for (int i = 0; i < layout->natts; i++)
		if (layout->cols[i].kind != LT_NONE)
			b->attidx[n++] = i;
	b->ncols = n;
	b->cols = palloc0(sizeof(ColBuf) * Max(n, 1));
	for (int i = 0; i < n; i++)
		colbuf_init(&b->cols[i], &layout->cols[b->attidx[i]]);
	b->utf8 = GetDatabaseEncoding() == PG_UTF8 || GetDatabaseEncoding() == PG_SQL_ASCII;
	b->tmpcxt = AllocSetContextCreate(CurrentMemoryContext, "lance_tam builder", ALLOCSET_DEFAULT_SIZES);
	return b;
}

int64
lt_builder_rows(LtBuilder *b)
{
	return b->nrows;
}

Size
lt_builder_bytes(LtBuilder *b)
{
	return b->bytes;
}

const uint64 *
lt_builder_rowids(LtBuilder *b)
{
	return (const uint64 *) b->rowids.data;
}

/* The bytes of a text value in UTF-8 */
static void
append_text_bytes(LtBuilder *b, ColBuf *c, const char *p, int len)
{
	int32		end;

	/*
	 * Lance's strings are UTF-8, which a reader checks: bytes that are not
	 * -- a SQL_ASCII database's, a C function's -- would make the column
	 * unreadable, and are refused here instead.
	 */
	if (b->utf8 && len > 0)
		(void) pg_verify_mbstr(PG_UTF8, p, len, false);
	else if (len > 0)
	{
		char	   *conv = (char *) pg_do_encoding_conversion((unsigned char *) p, len,
															   GetDatabaseEncoding(), PG_UTF8);

		if (conv != p)
		{
			len = strlen(conv);
			p = conv;
		}
	}
	if ((int64) c->values.len + len > PG_INT32_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("a batch of a lance table's text column exceeds 2 GB")));
	buf_append(&c->values, p, len);
	end = (int32) c->values.len;
	buf_append(&c->offsets, &end, sizeof(int32));
	b->bytes += len + sizeof(int32);
}

static void
append_empty_var(ColBuf *c)
{
	int32		end = (int32) c->values.len;

	buf_append(&c->offsets, &end, sizeof(int32));
}

/* numeric(p, s) as a decimal128 at scale s, from its text form */
static void
numeric_to_int128(Datum d, int scale, uint8 out[16])
{
	char	   *s = DatumGetCString(DirectFunctionCall1(numeric_out, d));
	const char *p = s;
	bool		neg = false;
	int			frac = -1;
	uint64		lo = 0;
	uint64		hi = 0;

	if (strcmp(s, "NaN") == 0 || strcmp(s, "Infinity") == 0 || strcmp(s, "-Infinity") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("%s cannot be stored in a lance decimal column", s)));
	if (*p == '-')
	{
		neg = true;
		p++;
	}
	/* 128-bit magnitude, as two 64-bit halves: v = v * 10 + digit */
	for (;; p++)
	{
		int			digit;
		unsigned __int128 v;

		if (*p == '.')
		{
			frac = 0;
			continue;
		}
		if (*p < '0' || *p > '9')
			break;
		if (frac >= 0)
		{
			if (frac >= scale)
				continue;		/* past the typmod's scale: never, after coercion */
			frac++;
		}
		digit = *p - '0';
		v = ((unsigned __int128) hi << 64) | lo;
		v = v * 10 + digit;
		hi = (uint64) (v >> 64);
		lo = (uint64) v;
	}
	if (frac < 0)
		frac = 0;
	for (; frac < scale; frac++)
	{
		unsigned __int128 v = ((unsigned __int128) hi << 64) | lo;

		v *= 10;
		hi = (uint64) (v >> 64);
		lo = (uint64) v;
	}
	{
		__int128	v = (__int128) (((unsigned __int128) hi << 64) | lo);

		if (neg)
			v = -v;
		memcpy(out, &v, 16);
	}
	pfree(s);
}

Datum
lt_int128_to_numeric(const uint8 in[16], int precision, int scale)
{
	__int128	v;
	unsigned __int128 mag;
	char		digits[64];
	char		buf[80];
	int			nd = 0;
	int			pos = 0;
	bool		neg;

	memcpy(&v, in, 16);
	neg = v < 0;
	mag = neg ? (unsigned __int128) (-(v + 1)) + 1 : (unsigned __int128) v;
	do
	{
		digits[nd++] = (char) ('0' + (int) (mag % 10));
		mag /= 10;
	} while (mag > 0 && nd < 60);
	while (nd <= scale)
		digits[nd++] = '0';
	if (neg)
		buf[pos++] = '-';
	for (int i = nd - 1; i >= 0; i--)
	{
		buf[pos++] = digits[i];
		if (i == scale && scale > 0)
			buf[pos++] = '.';
	}
	buf[pos] = '\0';
	return DirectFunctionCall3(numeric_in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(((precision << 16) | (scale & 0x7ff)) + VARHDRSZ));
}

/*
 * Dates and timestamps in Arrow's epoch, 1970; -infinity and +infinity as
 * their types' extremes in either epoch, as pg_vexec keeps them, which
 * order below and above every finite value as PostgreSQL's do.
 */
static int64
date_to_arrow(DateADT d)
{
	if (DATE_NOT_FINITE(d))
		return (int64) d;
	return (int64) d + DAYS_1970_TO_2000;
}

static int64
timestamp_to_arrow(Timestamp t)
{
	if (TIMESTAMP_NOT_FINITE(t))
		return t;
	/* from 294247-01-10 on, a value shifted would be +infinity's or past it */
	if (t >= PG_INT64_MAX - USECS_1970_TO_2000)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range for a lance table")));
	return t + USECS_1970_TO_2000;
}

static DateADT
date_from_arrow(int32 v)
{
	int64		d;

	if (v == PG_INT32_MIN || v == PG_INT32_MAX)
		return (DateADT) v;
	d = (int64) v - DAYS_1970_TO_2000;
	if (!IS_VALID_DATE(d))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("a lance table's date is out of PostgreSQL's range")));
	return (DateADT) d;
}

static Timestamp
timestamp_from_arrow(int64 v)
{
	if (v == PG_INT64_MIN || v == PG_INT64_MAX)
		return (Timestamp) v;
	if (v < PG_INT64_MIN + USECS_1970_TO_2000 || !IS_VALID_TIMESTAMP(v - USECS_1970_TO_2000))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("a lance table's timestamp is out of PostgreSQL's range")));
	return (Timestamp) (v - USECS_1970_TO_2000);
}

/* A scalar value of a kind, appended to a column's buffers */
static void
append_scalar(LtBuilder *b, ColBuf *c, LtKind kind, LtColumn *col, Datum d, bool isnull)
{
	int64		row = c->length;

	bits_set(&c->validity, row, !isnull);
	if (isnull)
		c->null_count++;
	switch (kind)
	{
		case LT_BOOL:
			bits_set(&c->values, row, isnull ? false : DatumGetBool(d));
			break;
		case LT_INT2:
			{
				int16		v = isnull ? 0 : DatumGetInt16(d);

				buf_append(&c->values, &v, sizeof(v));
				break;
			}
		case LT_INT4:
			{
				int32		v = isnull ? 0 : DatumGetInt32(d);

				buf_append(&c->values, &v, sizeof(v));
				break;
			}
		case LT_INT8:
			{
				int64		v = isnull ? 0 : DatumGetInt64(d);

				buf_append(&c->values, &v, sizeof(v));
				break;
			}
		case LT_FLOAT4:
			{
				float4		v = isnull ? 0 : DatumGetFloat4(d);

				buf_append(&c->values, &v, sizeof(v));
				break;
			}
		case LT_FLOAT8:
			{
				float8		v = isnull ? 0 : DatumGetFloat8(d);

				buf_append(&c->values, &v, sizeof(v));
				break;
			}
		case LT_NUMERIC:
			{
				uint8		v[16];

				memset(v, 0, sizeof(v));
				if (!isnull)
					numeric_to_int128(d, col->scale, v);
				buf_append(&c->values, v, 16);
				break;
			}
		case LT_DATE:
			{
				int32		v = isnull ? 0 : (int32) date_to_arrow(DatumGetDateADT(d));

				buf_append(&c->values, &v, sizeof(v));
				break;
			}
		case LT_TIME:
			{
				int64		v = isnull ? 0 : DatumGetTimeADT(d);

				buf_append(&c->values, &v, sizeof(v));
				break;
			}
		case LT_TIMESTAMP:
		case LT_TIMESTAMPTZ:
			{
				int64		v = isnull ? 0 : timestamp_to_arrow(DatumGetTimestamp(d));

				buf_append(&c->values, &v, sizeof(v));
				break;
			}
		case LT_UUID:
			if (isnull)
				buf_zero(&c->values, UUID_LEN);
			else
				buf_append(&c->values, DatumGetUUIDP(d)->data, UUID_LEN);
			break;
		case LT_TEXT:
		case LT_BYTEA:
			if (isnull)
				append_empty_var(c);
			else
			{
				struct varlena *v = PG_DETOAST_DATUM_PACKED(d);

				if (kind == LT_TEXT)
					append_text_bytes(b, c, VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
				else
				{
					int32		end;

					buf_append(&c->values, VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
					end = (int32) c->values.len;
					buf_append(&c->offsets, &end, sizeof(int32));
					b->bytes += VARSIZE_ANY_EXHDR(v);
				}
			}
			break;
		case LT_IO:
			if (isnull)
				append_empty_var(c);
			else
			{
				char	   *s = OutputFunctionCall(&col->out_fn, d);

				append_text_bytes(b, c, s, strlen(s));
			}
			break;
		default:
			elog(ERROR, "lance_tam: kind %d is not a scalar", kind);
	}
	c->length++;
	b->bytes += 8;
}

static void
append_vector(LtBuilder *b, ColBuf *c, LtColumn *col, Datum d, bool isnull)
{
	int64		row = c->length;
	size_t		width = col->kind == LT_HALFVEC ? 2 : 4;

	bits_set(&c->validity, row, !isnull);
	if (isnull)
	{
		c->null_count++;
		buf_zero(&c->child->values, width * col->dim);
	}
	else
	{
		struct varlena *v = PG_DETOAST_DATUM(d);
		int16		dim = ((PgVector *) v)->dim;

		if (dim != col->dim)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("expected %d dimensions, not %d", col->dim, dim)));
		if (col->kind == LT_HALFVEC)
			buf_append(&c->child->values, ((PgHalfVec *) v)->x, width * dim);
		else
			buf_append(&c->child->values, ((PgVector *) v)->x, width * dim);
	}
	c->child->length += col->dim;
	c->length++;
	b->bytes += width * col->dim;
}

static void
append_array(LtBuilder *b, ColBuf *c, LtColumn *col, Datum d, bool isnull)
{
	int64		row = c->length;
	int32		end;

	bits_set(&c->validity, row, !isnull);
	if (isnull)
		c->null_count++;
	else
	{
		ArrayType  *arr = DatumGetArrayTypeP(d);
		Datum	   *elems;
		bool	   *nulls;
		int			n;

		if (ARR_NDIM(arr) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("multidimensional arrays cannot be stored in a lance table")));
		deconstruct_array(arr, ARR_ELEMTYPE(arr), col->elem_len, col->elem_byval, col->elem_align,
						  &elems, &nulls, &n);
		for (int i = 0; i < n; i++)
			append_scalar(b, c->child, col->elem_kind, col, elems[i], nulls[i]);
	}
	end = (int32) c->child->length;
	buf_append(&c->offsets, &end, sizeof(int32));
	c->length++;
}

void
lt_builder_add(LtBuilder *b, TupleTableSlot *slot, uint64 rowid)
{
	MemoryContext old;

	slot_getallattrs(slot);
	old = MemoryContextSwitchTo(b->tmpcxt);
	for (int i = 0; i < b->ncols; i++)
	{
		int			ai = b->attidx[i];
		LtColumn   *col = &b->layout->cols[ai];
		ColBuf	   *c = &b->cols[i];
		Datum		d = slot->tts_values[ai];
		bool		isnull = slot->tts_isnull[ai];

		switch (col->kind)
		{
			case LT_VECTOR:
			case LT_HALFVEC:
				append_vector(b, c, col, d, isnull);
				break;
			case LT_ARRAY:
				append_array(b, c, col, d, isnull);
				break;
			default:
				append_scalar(b, c, col->kind, col, d, isnull);
				break;
		}
	}
	MemoryContextSwitchTo(old);
	MemoryContextReset(b->tmpcxt);
	buf_append(&b->rowids, &rowid, sizeof(uint64));
	b->nrows++;
}

/* An exported array's private data: the buffers it owns */
typedef struct ArrayPrivate
{
	const void *buffers[3];
	void	   *owned[3];
	struct ArrowArray **children;
	struct ArrowArray *child_storage;
} ArrayPrivate;

static void
array_release(struct ArrowArray *array)
{
	ArrayPrivate *p = (ArrayPrivate *) array->private_data;

	if (array->release == NULL)
		return;
	for (int64 i = 0; i < array->n_children; i++)
	{
		struct ArrowArray *child = array->children[i];

		if (child->release)
			child->release(child);
	}
	if (p)
	{
		for (int i = 0; i < 3; i++)
			free(p->owned[i]);
		free(p->children);
		free(p->child_storage);
		free(p);
	}
	array->release = NULL;
}

/*
 * Move a column's buffers into an exported array.  Arrow wants the buffers
 * 8-byte aligned at least; malloc's are 16.
 */
static void
array_from_colbuf(struct ArrowArray *a, ColBuf *c, LtColumn *col, LtKind kind, bool is_child)
{
	ArrayPrivate *p = calloc(1, sizeof(ArrayPrivate));
	int			nchildren = 0;

	if (p == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	memset(a, 0, sizeof(*a));
	a->length = c->length;
	a->null_count = c->null_count;
	a->offset = 0;

	/* validity, absent when nothing is NULL */
	if (c->null_count > 0)
	{
		buf_reserve(&c->validity, 8);
		p->owned[0] = c->validity.data;
		p->buffers[0] = c->validity.data;
	}
	else
	{
		free(c->validity.data);
		p->buffers[0] = NULL;
	}
	c->validity.data = NULL;

	if (!is_child && (kind == LT_VECTOR || kind == LT_HALFVEC))
	{
		a->n_buffers = 1;
		nchildren = 1;
	}
	else if (!is_child && kind == LT_ARRAY)
	{
		a->n_buffers = 2;
		p->owned[1] = c->offsets.data;
		p->buffers[1] = c->offsets.data;
		c->offsets.data = NULL;
		nchildren = 1;
	}
	else if (kind == LT_TEXT || kind == LT_BYTEA || kind == LT_IO)
	{
		a->n_buffers = 3;
		buf_reserve(&c->values, 8);
		p->owned[1] = c->offsets.data;
		p->buffers[1] = c->offsets.data;
		p->owned[2] = c->values.data;
		p->buffers[2] = c->values.data;
		c->offsets.data = NULL;
		c->values.data = NULL;
	}
	else
	{
		a->n_buffers = 2;
		buf_reserve(&c->values, 16);
		p->owned[1] = c->values.data;
		p->buffers[1] = c->values.data;
		c->values.data = NULL;
	}
	a->buffers = p->buffers;
	if (nchildren > 0)
	{
		LtKind		ck = kind == LT_ARRAY ? col->elem_kind : (kind == LT_HALFVEC ? LT_INT2 : LT_FLOAT4);

		p->child_storage = calloc(1, sizeof(struct ArrowArray));
		p->children = calloc(1, sizeof(struct ArrowArray *));
		if (p->child_storage == NULL || p->children == NULL)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
		p->children[0] = p->child_storage;
		array_from_colbuf(p->child_storage, c->child, col, ck, true);
		a->n_children = 1;
		a->children = p->children;
	}
	a->release = array_release;
	a->private_data = p;
}

void
lt_builder_export(LtBuilder *b, struct ArrowArray *array, struct ArrowSchema *schema)
{
	ArrayPrivate *p = calloc(1, sizeof(ArrayPrivate));

	if (p == NULL)
		ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	lt_export_schema(b->layout, b->ncols, b->attidx, schema);

	memset(array, 0, sizeof(*array));
	array->length = b->nrows;
	array->null_count = 0;
	array->n_buffers = 1;
	p->buffers[0] = NULL;
	array->buffers = p->buffers;
	array->n_children = b->ncols;
	if (b->ncols > 0)
	{
		p->child_storage = calloc(b->ncols, sizeof(struct ArrowArray));
		p->children = calloc(b->ncols, sizeof(struct ArrowArray *));
		if (p->child_storage == NULL || p->children == NULL)
			ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
	}
	array->children = p->children;
	array->release = array_release;
	array->private_data = p;
	for (int i = 0; i < b->ncols; i++)
	{
		LtColumn   *col = &b->layout->cols[b->attidx[i]];

		p->children[i] = &p->child_storage[i];
		array_from_colbuf(&p->child_storage[i], &b->cols[i], col, col->kind, false);
		/* the buffers moved; the column starts over */
		colbuf_free(&b->cols[i]);
		colbuf_init(&b->cols[i], col);
	}
}

void
lt_builder_reset(LtBuilder *b)
{
	for (int i = 0; i < b->ncols; i++)
	{
		colbuf_free(&b->cols[i]);
		colbuf_init(&b->cols[i], &b->layout->cols[b->attidx[i]]);
	}
	b->rowids.len = 0;
	b->nrows = 0;
	b->bytes = 0;
}

void
lt_builder_free(LtBuilder *b)
{
	if (b == NULL)
		return;
	for (int i = 0; i < b->ncols; i++)
		colbuf_free(&b->cols[i]);
	free(b->rowids.data);
	b->rowids.data = NULL;
	MemoryContextDelete(b->tmpcxt);
	pfree(b->cols);
	pfree(b->attidx);
	pfree(b);
}

/* ------------------------------------------------------------------------
 * Batches read
 * ------------------------------------------------------------------------
 */

void
lt_batch_release(LtBatch *batch)
{
	if (!batch->valid)
		return;
	if (batch->array.release)
		batch->array.release(&batch->array);
	if (batch->schema.release)
		batch->schema.release(&batch->schema);
	batch->valid = false;
	batch->nrows = 0;
}

void
lt_batch_bind(LtBatch *batch, int ncols, LtColumn **cols)
{
	batch->ncols = ncols;
	batch->cols = cols;
	batch->nrows = batch->array.length;
	batch->rowid_index = -1;
	batch->distance_index = -1;
	batch->score_index = -1;
	for (int64 i = 0; i < batch->schema.n_children; i++)
	{
		const char *name = batch->schema.children[i]->name;

		if (i < ncols || name == NULL)
			continue;
		if (strcmp(name, "_rowid") == 0)
			batch->rowid_index = (int) i;
		else if (strcmp(name, "_distance") == 0)
			batch->distance_index = (int) i;
		else if (strcmp(name, "_score") == 0)
			batch->score_index = (int) i;
	}
}

static inline bool
bit_get(const void *bits, int64 i)
{
	return (((const uint8 *) bits)[i >> 3] >> (i & 7)) & 1;
}

static inline bool
array_isnull(const struct ArrowArray *a, int64 row)
{
	const void *validity = a->n_buffers > 0 ? a->buffers[0] : NULL;

	if (a->null_count == 0 || validity == NULL)
		return false;
	return !bit_get(validity, a->offset + row);
}

bool
lt_batch_isnull(LtBatch *batch, int col, int64 row)
{
	return array_isnull(batch->array.children[col], row);
}

uint64
lt_batch_rowid(LtBatch *batch, int64 row)
{
	const struct ArrowArray *a;

	if (batch->rowid_index < 0)
		elog(ERROR, "lance_tam: the batch has no _rowid");
	a = batch->array.children[batch->rowid_index];
	return ((const uint64 *) a->buffers[1])[a->offset + row];
}

float4
lt_batch_float(LtBatch *batch, int child, int64 row, bool *isnull)
{
	const struct ArrowArray *a = batch->array.children[child];

	*isnull = array_isnull(a, row);
	if (*isnull)
		return 0;
	return ((const float4 *) a->buffers[1])[a->offset + row];
}

static Datum
text_datum(const char *p, int32 len)
{
	if (GetDatabaseEncoding() != PG_UTF8 && GetDatabaseEncoding() != PG_SQL_ASCII && len > 0)
	{
		char	   *conv = (char *) pg_do_encoding_conversion((unsigned char *) p, len,
															   PG_UTF8, GetDatabaseEncoding());

		if (conv != p)
			return PointerGetDatum(cstring_to_text(conv));
	}
	return PointerGetDatum(cstring_to_text_with_len(p, len));
}

/* A scalar of a kind at index i of an array (its offset applied) */
static Datum
scalar_datum(const struct ArrowArray *a, int64 i, LtKind kind, LtColumn *col)
{
	int64		k = a->offset + i;

	switch (kind)
	{
		case LT_BOOL:
			return BoolGetDatum(bit_get(a->buffers[1], k));
		case LT_INT2:
			return Int16GetDatum(((const int16 *) a->buffers[1])[k]);
		case LT_INT4:
			return Int32GetDatum(((const int32 *) a->buffers[1])[k]);
		case LT_INT8:
			return Int64GetDatum(((const int64 *) a->buffers[1])[k]);
		case LT_FLOAT4:
			return Float4GetDatum(((const float4 *) a->buffers[1])[k]);
		case LT_FLOAT8:
			return Float8GetDatum(((const float8 *) a->buffers[1])[k]);
		case LT_NUMERIC:
			return lt_int128_to_numeric(((const uint8 *) a->buffers[1]) + 16 * k,
									 col->precision, col->scale);
		case LT_DATE:
			return DateADTGetDatum(date_from_arrow(((const int32 *) a->buffers[1])[k]));
		case LT_TIME:
			return TimeADTGetDatum(((const int64 *) a->buffers[1])[k]);
		case LT_TIMESTAMP:
		case LT_TIMESTAMPTZ:
			return TimestampGetDatum(timestamp_from_arrow(((const int64 *) a->buffers[1])[k]));
		case LT_UUID:
			{
				pg_uuid_t  *u = palloc(sizeof(pg_uuid_t));

				memcpy(u->data, ((const uint8 *) a->buffers[1]) + UUID_LEN * k, UUID_LEN);
				return UUIDPGetDatum(u);
			}
		case LT_TEXT:
		case LT_BYTEA:
		case LT_IO:
			{
				const int32 *off = (const int32 *) a->buffers[1];
				const char *data = (const char *) a->buffers[2];
				int32		start = off[k];
				int32		len = off[k + 1] - start;

				if (kind == LT_TEXT)
					return text_datum(data + start, len);
				if (kind == LT_BYTEA)
				{
					bytea	   *v = palloc(VARHDRSZ + len);

					SET_VARSIZE(v, VARHDRSZ + len);
					memcpy(VARDATA(v), data + start, len);
					return PointerGetDatum(v);
				}
				else
				{
					char	   *s = palloc(len + 1);

					memcpy(s, data + start, len);
					s[len] = '\0';
					if (GetDatabaseEncoding() != PG_UTF8 && GetDatabaseEncoding() != PG_SQL_ASCII)
						s = (char *) pg_do_encoding_conversion((unsigned char *) s, len,
															   PG_UTF8, GetDatabaseEncoding());
					return InputFunctionCall(&col->in_fn, s, col->ioparam, col->typmod);
				}
			}
		default:
			elog(ERROR, "lance_tam: kind %d is not a scalar", kind);
	}
	pg_unreachable();
}

Datum
lt_batch_datum(LtBatch *batch, int colno, int64 row, bool *isnull)
{
	const struct ArrowArray *a = batch->array.children[colno];
	LtColumn   *col = batch->cols[colno];

	if (col == NULL || array_isnull(a, row))
	{
		*isnull = true;
		return (Datum) 0;
	}
	*isnull = false;
	switch (col->kind)
	{
		case LT_VECTOR:
		case LT_HALFVEC:
			{
				const struct ArrowArray *child = a->children[0];
				size_t		width = col->kind == LT_HALFVEC ? 2 : 4;
				int64		start = (a->offset + row) * col->dim + child->offset;
				Size		size = offsetof(PgVector, x) + width * col->dim;
				PgVector   *v = palloc0(size);

				SET_VARSIZE(v, size);
				v->dim = col->dim;
				memcpy(v->x, ((const char *) child->buffers[1]) + width * start, width * col->dim);
				return PointerGetDatum(v);
			}
		case LT_ARRAY:
			{
				const struct ArrowArray *child = a->children[0];
				const int32 *off = (const int32 *) a->buffers[1];
				int32		start = off[a->offset + row];
				int32		n = off[a->offset + row + 1] - start;
				Datum	   *elems = palloc(sizeof(Datum) * Max(n, 1));
				bool	   *nulls = palloc(sizeof(bool) * Max(n, 1));
				int			dims[1];
				int			lbs[1];

				for (int i = 0; i < n; i++)
				{
					nulls[i] = array_isnull(child, start + i);
					elems[i] = nulls[i] ? (Datum) 0 :
						scalar_datum(child, start + i, col->elem_kind, col);
				}
				dims[0] = n;
				lbs[0] = 1;
				return PointerGetDatum(construct_md_array(elems, nulls, n > 0 ? 1 : 0, dims, lbs,
														  col->elem_type, col->elem_len,
														  col->elem_byval, col->elem_align));
			}
		default:
			return scalar_datum(a, row, col->kind, col);
	}
}

/* ------------------------------------------------------------------------
 * A dataset's column as a PostgreSQL type, for CREATE TABLE ... () with a
 * dataset that exists
 * ------------------------------------------------------------------------
 */

static const char *
scalar_type_name(const char *f)
{
	if (strcmp(f, "b") == 0)
		return "boolean";
	if (strcmp(f, "c") == 0 || strcmp(f, "C") == 0 || strcmp(f, "s") == 0)
		return "smallint";
	if (strcmp(f, "S") == 0 || strcmp(f, "i") == 0)
		return "integer";
	if (strcmp(f, "I") == 0 || strcmp(f, "l") == 0)
		return "bigint";
	if (strcmp(f, "L") == 0)
		return "numeric(20,0)";
	if (strcmp(f, "e") == 0 || strcmp(f, "f") == 0)
		return "real";
	if (strcmp(f, "g") == 0)
		return "double precision";
	if (strcmp(f, "u") == 0 || strcmp(f, "U") == 0 || strcmp(f, "vu") == 0)
		return "text";
	if (strcmp(f, "z") == 0 || strcmp(f, "Z") == 0 || strcmp(f, "vz") == 0)
		return "bytea";
	if (strcmp(f, "tdD") == 0 || strcmp(f, "tdm") == 0)
		return "date";
	if (strncmp(f, "tt", 2) == 0)
		return "time";
	if (strncmp(f, "ts", 2) == 0 && strlen(f) >= 4)
		return f[4] == '\0' ? "timestamp" : "timestamptz";
	if (strcmp(f, "w:16") == 0)
		return "uuid";
	if (strncmp(f, "w:", 2) == 0)
		return "bytea";
	return NULL;
}

char *
lt_arrow_to_pg_type(const struct ArrowSchema *field, char **comment)
{
	const char *f = field->format;
	const char *scalar;
	bool		have_vector = OidIsValid(TypenameGetTypid("vector"));
	bool		have_halfvec = OidIsValid(TypenameGetTypid("halfvec"));

	*comment = NULL;
	if (strncmp(f, "d:", 2) == 0)
	{
		int			p = 0;
		int			s = 0;

		if (sscanf(f, "d:%d,%d", &p, &s) == 2 && p >= 1 && p <= 1000 && s >= 0 && s <= p)
			return psprintf("numeric(%d,%d)", p, s);
		return pstrdup("numeric");
	}
	scalar = scalar_type_name(f);
	if (scalar)
		return pstrdup(scalar);
	if (strncmp(f, "+w:", 3) == 0 && field->n_children == 1)
	{
		int			dim = atoi(f + 3);
		const char *cf = field->children[0]->format;

		if (strcmp(cf, "f") == 0 && have_vector && dim > 0 && dim <= 16000)
			return psprintf("vector(%d)", dim);
		if (strcmp(cf, "e") == 0 && have_halfvec && dim > 0 && dim <= 16000)
			return psprintf("halfvec(%d)", dim);
		scalar = scalar_type_name(cf);
		if (scalar && strchr(scalar, '(') == NULL)
			return psprintf("%s[]", scalar);
		return NULL;
	}
	if ((strcmp(f, "+l") == 0 || strcmp(f, "+L") == 0 || strcmp(f, "+vl") == 0) &&
		field->n_children == 1)
	{
		const char *cf = field->children[0]->format;

		if (strncmp(cf, "d:", 2) == 0)
			return pstrdup("numeric[]");
		scalar = scalar_type_name(cf);
		if (scalar && strchr(scalar, '(') == NULL)
			return psprintf("%s[]", scalar);
		return NULL;
	}
	*comment = psprintf("Arrow type \"%s\"", f);
	return NULL;
}
