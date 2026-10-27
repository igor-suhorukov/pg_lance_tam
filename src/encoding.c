/*-------------------------------------------------------------------------
 *
 * encoding.c
 *	  How Lance encodes a table's columns (docs/design.md §2.5): the
 *	  table's options compression, compression_level and column_encoding
 *	  as the "lance-encoding:" metadata of the dataset's fields, which
 *	  Lance's writers read for every version written after.
 *
 *	  compression and compression_level apply to every column; an entry of
 *	  column_encoding -- a JSON object from a column's name to an object of
 *	  Lance's keys and their values -- to its column alone, over them.  The
 *	  metadata goes on a column's field and, for an array or a vector, on
 *	  its item, the field whose values Lance encodes.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"

#include "lance_tam.h"

#define ENCODING_PREFIX "lance-encoding:"

/* Lance's keys of a field's encoding: lance-encoding's constants.rs */
static const char *const encoding_keys[] = {
	"compression", "compression-level", "rle-threshold", "minichunk-size",
	"dict-divisor", "dict-size-ratio", "dict-values-compression",
	"dict-values-compression-level", "packed", "structural-encoding", "bss", NULL
};

/* Lance's general compression schemes: CompressionScheme::from_str */
static const char *const compression_schemes[] = {"lz4", "zstd", "none", "fsst", NULL};

typedef struct EncodingEntry
{
	char	   *column;			/* NULL: every column */
	char	   *key;			/* without the prefix */
	char	   *value;
} EncodingEntry;

static bool
in_list(const char *const *list, const char *s)
{
	for (int i = 0; list[i]; i++)
		if (strcmp(list[i], s) == 0)
			return true;
	return false;
}

/* A key as Lance's metadata names it, the prefix optional */
static char *
encoding_key(const char *key)
{
	const char *bare = strncmp(key, ENCODING_PREFIX, strlen(ENCODING_PREFIX)) == 0
		? key + strlen(ENCODING_PREFIX) : key;

	if (!in_list(encoding_keys, bare))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a key of Lance's column encodings", key),
				 errhint("The keys are compression, compression-level, rle-threshold, minichunk-size, dict-divisor, dict-size-ratio, dict-values-compression, dict-values-compression-level, packed, structural-encoding and bss.")));
	return pstrdup(bare);
}

static void
check_value(const char *what, const char *key, const char *value)
{
	if (strcmp(key, "compression") == 0 || strcmp(key, "dict-values-compression") == 0)
	{
		if (!in_list(compression_schemes, value))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s: \"%s\" is not a compression of Lance's", what, value),
					 errhint("Lance compresses with lz4, zstd or fsst, or none.")));
	}
	else if (strcmp(key, "compression-level") == 0 ||
			 strcmp(key, "dict-values-compression-level") == 0)
	{
		char	   *end;
		long		v;

		errno = 0;
		v = strtol(value, &end, 10);
		if (errno != 0 || end == value || *end != '\0' || v < INT_MIN || v > INT_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s: a compression level must be an integer, not \"%s\"", what, value)));
	}
}

/* A scalar of column_encoding as text: a string, a number or a boolean */
static char *
scalar_text(JsonbValue *v, const char *column, const char *key)
{
	switch (v->type)
	{
		case jbvString:
			return pnstrdup(v->val.string.val, v->val.string.len);
		case jbvNumeric:
			return DatumGetCString(DirectFunctionCall1(numeric_out,
													   NumericGetDatum(v->val.numeric)));
		case jbvBool:
			return pstrdup(v->val.boolean ? "true" : "false");
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("column_encoding: the value of \"%s\" of column \"%s\" must be a string, a number or a boolean",
							key, column)));
	}
	return NULL;
}

pg_noreturn static void
not_an_object(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("column_encoding must be a JSON object of a column's name to an object of Lance's keys"),
			 errhint("For example: column_encoding = '{\"ids\": {\"compression\": \"zstd\"}}'.")));
}

/* column_encoding's entries, in its order */
static List *
parse_column_encoding(const char *json)
{
	Jsonb	   *jb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(json)));
	JsonbIterator *it;
	JsonbValue	v;
	JsonbIteratorToken tok;
	int			depth = 0;
	char	   *column = NULL;
	char	   *key = NULL;
	List	   *entries = NIL;

	if (!JB_ROOT_IS_OBJECT(jb) || JB_ROOT_IS_SCALAR(jb))
		not_an_object();
	it = JsonbIteratorInit(&jb->root);
	while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		switch (tok)
		{
			case WJB_BEGIN_OBJECT:
				if (++depth > 2)
					not_an_object();
				break;
			case WJB_END_OBJECT:
				depth--;
				break;
			case WJB_KEY:
				if (depth == 1)
					column = pnstrdup(v.val.string.val, v.val.string.len);
				else
					key = pnstrdup(v.val.string.val, v.val.string.len);
				break;
			case WJB_VALUE:
				{
					EncodingEntry *e;

					if (depth != 2)
						not_an_object();
					e = palloc(sizeof(EncodingEntry));
					e->column = column;
					e->key = encoding_key(key);
					e->value = scalar_text(&v, column, key);
					check_value("column_encoding", e->key, e->value);
					entries = lappend(entries, e);
				}
				break;
			default:
				not_an_object();
		}
	}
	return entries;
}

/* A table option of the encodings, checked when the table is created */
void
lt_check_encoding_option(DefElem *d)
{
	char	   *value = defGetString(d);

	if (strcmp(d->defname, "compression") == 0)
		check_value("option \"compression\"", "compression", value);
	else if (strcmp(d->defname, "compression_level") == 0)
		check_value("option \"compression_level\"", "compression-level", value);
	else if (strcmp(d->defname, "column_encoding") == 0)
		(void) parse_column_encoding(value);
}

/* The entries for every column, then column_encoding's */
static List *
encoding_entries(List *options)
{
	char	   *compression = lt_option_string(options, "compression");
	char	   *level = lt_option_string(options, "compression_level");
	char	   *json = lt_option_string(options, "column_encoding");
	List	   *entries = NIL;

	if (compression)
	{
		EncodingEntry *e = palloc0(sizeof(EncodingEntry));

		e->key = "compression";
		e->value = compression;
		entries = lappend(entries, e);
	}
	if (level)
	{
		EncodingEntry *e = palloc0(sizeof(EncodingEntry));

		e->key = "compression-level";
		e->value = level;
		entries = lappend(entries, e);
	}
	if (json)
		entries = list_concat(entries, parse_column_encoding(json));
	return entries;
}

/*
 * A column's metadata in the Arrow C data interface's form: an int32 count,
 * then each key and value as an int32 length and its bytes.  NULL when the
 * column has no entry.
 */
static char *
column_metadata(List *entries, const char *column, size_t *len)
{
	List	   *keys = NIL;
	List	   *values = NIL;
	ListCell   *lc;
	StringInfoData buf;
	int32		n;

	foreach(lc, entries)
	{
		EncodingEntry *e = lfirst(lc);
		ListCell   *k;
		ListCell   *v;
		bool		replaced = false;

		if (e->column && strcmp(e->column, column) != 0)
			continue;
		forboth(k, keys, v, values)
		{
			if (strcmp(lfirst(k), e->key) == 0)
			{
				lfirst(v) = e->value;
				replaced = true;
			}
		}
		if (!replaced)
		{
			keys = lappend(keys, e->key);
			values = lappend(values, e->value);
		}
	}
	if (keys == NIL)
		return NULL;

	initStringInfo(&buf);
	n = list_length(keys);
	appendBinaryStringInfo(&buf, &n, sizeof(n));
	{
		ListCell   *k;
		ListCell   *v;

		forboth(k, keys, v, values)
		{
			char	   *key = psprintf(ENCODING_PREFIX "%s", (char *) lfirst(k));
			int32		klen = strlen(key);
			int32		vlen = strlen(lfirst(v));

			appendBinaryStringInfo(&buf, &klen, sizeof(klen));
			appendBinaryStringInfo(&buf, key, klen);
			appendBinaryStringInfo(&buf, &vlen, sizeof(vlen));
			appendBinaryStringInfo(&buf, lfirst(v), vlen);
		}
	}
	*len = buf.len;
	return buf.data;
}

/*
 * The encodings of the table's options on an exported struct schema of its
 * columns.  With every_column, a column column_encoding names that the
 * schema lacks is an error: a new table names all its columns, ADD COLUMN
 * only the new ones.
 */
void
lt_schema_set_encoding(struct ArrowSchema *schema, List *options, bool every_column)
{
	List	   *entries = encoding_entries(options);
	ListCell   *lc;

	if (entries == NIL)
		return;
	if (every_column)
	{
		foreach(lc, entries)
		{
			EncodingEntry *e = lfirst(lc);
			bool		found = e->column == NULL;

			for (int64 i = 0; !found && i < schema->n_children; i++)
				found = strcmp(schema->children[i]->name, e->column) == 0;
			if (!found)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column_encoding names column \"%s\", which the table does not have",
								e->column)));
		}
	}
	for (int64 i = 0; i < schema->n_children; i++)
	{
		struct ArrowSchema *field = schema->children[i];
		size_t		len = 0;
		char	   *metadata = column_metadata(entries, field->name, &len);

		if (metadata == NULL)
			continue;
		lt_schema_set_metadata(field, metadata, len);
		for (int64 j = 0; j < field->n_children; j++)
			lt_schema_set_metadata(field->children[j], metadata, len);
	}
}

/* An Arrow field's metadata as a JSON object, appended; nothing for none */
void
lt_append_arrow_metadata(StringInfo buf, const char *metadata)
{
	int32		n;
	const char *p = metadata;

	appendStringInfoChar(buf, '{');
	if (metadata)
	{
		memcpy(&n, p, sizeof(n));
		p += sizeof(n);
		for (int32 i = 0; i < n; i++)
		{
			int32		klen;
			int32		vlen;
			char	   *key;
			char	   *value;

			memcpy(&klen, p, sizeof(klen));
			p += sizeof(klen);
			key = pnstrdup(p, klen);
			p += klen;
			memcpy(&vlen, p, sizeof(vlen));
			p += sizeof(vlen);
			value = pnstrdup(p, vlen);
			p += vlen;
			if (i > 0)
				appendStringInfoChar(buf, ',');
			escape_json(buf, key);
			appendStringInfoChar(buf, ':');
			escape_json(buf, value);
		}
	}
	appendStringInfoChar(buf, '}');
}
