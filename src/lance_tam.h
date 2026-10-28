/*-------------------------------------------------------------------------
 *
 * lance_tam.h
 *	  The extension's shared declarations (docs/design.md §2).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#ifndef LANCE_TAM_H
#define LANCE_TAM_H

#include "postgres.h"

#include "access/tableam.h"
#include "fmgr.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "storage/itemptr.h"
#include "utils/rel.h"
#include "utils/hsearch.h"
#include "utils/snapshot.h"

#include "lance_engine.h"

/*
 * Row ids and TIDs (§2.6).  A row's id is the Lance stable row id the
 * server gave it at insert; its TID is that id, 256 rows a block, so that a
 * TID's offset stays within MaxHeapTuplesPerPage, as TID bitmaps ask.  The
 * ids the server gives start at 2^39, above the ids Lance gives the rows
 * other writers append.
 */
#define LT_ROWS_PER_BLOCK	256
#define LT_ROWID_START		(UINT64CONST(1) << 39)
#define LT_ROWID_LIMIT		(UINT64CONST(1) << 40)
#define LT_ROWID_CHUNK		65536

static inline void
lt_rowid_to_tid(uint64 rowid, ItemPointer tid)
{
	ItemPointerSet(tid, (BlockNumber) (rowid >> 8), (OffsetNumber) ((rowid & 0xFF) + 1));
}

static inline uint64
lt_tid_to_rowid(ItemPointer tid)
{
	return ((uint64) ItemPointerGetBlockNumberNoCheck(tid) << 8) |
		(uint64) (ItemPointerGetOffsetNumberNoCheck(tid) - 1);
}

static inline bool
lt_tid_valid(ItemPointer tid)
{
	OffsetNumber off = ItemPointerGetOffsetNumberNoCheck(tid);

	return off >= 1 && off <= LT_ROWS_PER_BLOCK;
}

/* module.c: settings */
extern char *lt_engine_library;
extern char *lt_default_location;
extern char *lt_default_server;
extern int	lt_worker_threads;
extern int	lt_index_cache_mb;
extern int	lt_metadata_cache_mb;
extern int	lt_batch_rows;
extern int	lt_readahead;
extern int	lt_buffer_rows;
extern int	lt_nprobes;
extern int	lt_ef;
extern int	lt_refine_factor;
extern double lt_scan_start_cost;
extern bool lt_enable_customscan;
extern bool lt_enable_ann;
extern bool lt_enable_filter_pushdown;
extern bool lt_update_in_place;
extern bool lt_scan_in_order;
extern bool lt_vacuum_compact;
extern int	lt_version_retention;
extern int	lt_debug_fail_after_commit;
extern bool lt_preloaded;

/* engine.c */
extern const LeApi *lt_api(void);
extern bool lt_engine_loaded(void);
pg_noreturn extern void lt_raise(LeError *err, const char *context);
extern void lt_warn(LeError *err, const char *context);
extern char *lt_take_string(char *s);

/* tableam.c */
struct LtReader;
struct LtReadSpec;
extern const TableAmRoutine *lt_tableam(void);
extern struct LtReader *lt_scan_take_reader(TableScanDesc scan, struct LtReadSpec *spec);
extern void lt_scan_reset_take(TableScanDesc scan);
extern const TupleTableSlotOps *lt_slot_ops_of(void);
extern bool lt_is_lance_rel(Relation rel);
extern bool lt_is_lance_relid(Oid relid);
extern Oid	lt_am_oid(void);

/*
 * catalog.c: what lance.tables says of a table (§2.6).  The fixed part is
 * kept per backend until the relation changes; the version is the catalog
 * row's that a snapshot sees.
 */
typedef struct LtTable
{
	Oid			relid;
	char	   *uri;
	char	   *storage_json;	/* storage options, a JSON object */
	char	   *server;			/* the storage server, or NULL */
	bool		managed;		/* the server made the dataset; DROP removes
								 * it */
	bool		writable;
	bool		follow;			/* read-only: read Lance's latest version */
	int			natts;
	char	  **columns;		/* the Lance column of each attribute, NULL
								 * for none */
	int			max_rows_per_file;
	int			max_rows_per_group;
	int64		buffer_rows;
	char	   *storage_version;
	List	   *options;		/* the table's options, DefElems */
} LtTable;

extern Oid	lt_catalog_relid(void);
extern LtTable *lt_table(Relation rel);
extern LtTable *lt_table_by_relid(Oid relid, bool missing_ok);
extern LtTable *lt_table_copy_by_relid(Oid relid, bool missing_ok);
extern uint64 lt_catalog_version(Oid relid, Snapshot snapshot);
extern uint64 lt_snapshot_version(Relation rel, LtTable *t, Snapshot snapshot);
extern void lt_catalog_insert(Oid relid, const char *uri, uint64 version, bool managed,
							  bool writable, bool follow, List *columns, List *options,
							  const char *server);
extern void lt_catalog_set_version(Oid relid, uint64 version);
extern void lt_catalog_set_columns(Oid relid, List *columns);
extern void lt_catalog_delete(Oid relid);
extern void lt_table_invalidate(Oid relid);
extern char *lt_storage_json(List *options, const char *server, Oid owner);
extern uint64 lt_next_rowid(void);
extern void lt_rowids_floor(uint64 max_row_id);
extern void lt_lock_table_commit(Oid relid);

/*
 * catalog.c: what the planner costs a scan of a version of a table's
 * dataset by (customscan.c) -- its fragments, which a parallel scan's
 * participants share out, and its Lance indices -- and its fields that
 * filters may compare with timestamps (filter.c), kept with the table's
 * row: a version's never change.
 */
typedef enum LtIndexKind
{
	LT_INDEX_OTHER = 0,			/* one a filter's cost does not count on */
	LT_INDEX_BTREE,
	LT_INDEX_BITMAP,
	LT_INDEX_LABEL_LIST			/* a list column's, of its elements */
} LtIndexKind;

typedef struct LtIndexInfo
{
	char	   *name;
	AttrNumber	attnum;			/* the attribute it indexes, 0: none */
	LtIndexKind kind;
	double		rows;			/* the version's live rows it covers */
} LtIndexInfo;

typedef struct LtVersionInfo
{
	uint64		version;
	uint64		nfragments;
	int			nindices;
	LtIndexInfo *indices;
	/*
	 * The attributes whose field is the server's timestamp[us] without a
	 * time zone, and with UTC: filters compare them with literals
	 */
	Bitmapset  *timestamps;
	Bitmapset  *timestamptzs;
} LtVersionInfo;

extern LtVersionInfo *lt_version_info(Relation rel, uint64 version);
extern Bitmapset *lt_table_inplace_blocking(Relation rel);

/* convert.c: attributes as Arrow columns (§2.5) */
typedef enum LtKind
{
	LT_NONE = 0,				/* a dropped attribute */
	LT_BOOL,
	LT_INT2,
	LT_INT4,
	LT_INT8,
	LT_FLOAT4,
	LT_FLOAT8,
	LT_NUMERIC,					/* numeric(p, s), p <= 38: decimal128 */
	LT_TEXT,					/* text, varchar, bpchar, name: utf8 */
	LT_BYTEA,
	LT_DATE,
	LT_TIME,
	LT_TIMESTAMP,
	LT_TIMESTAMPTZ,
	LT_UUID,
	LT_VECTOR,					/* pgvector's vector(n): fixed-size list of
								 * float32 */
	LT_HALFVEC,					/* pgvector's halfvec(n): of float16 */
	LT_ARRAY,					/* a one-dimensional array of a kind above:
								 * list */
	LT_IO						/* any other type, as its text form: utf8 */
} LtKind;

typedef struct LtColumn
{
	AttrNumber	attnum;
	const char *name;			/* the Lance column */
	Oid			typid;			/* the base type, domains looked through */
	int32		typmod;
	LtKind		kind;
	int32		dim;			/* vectors */
	int32		precision;		/* numeric */
	int32		scale;
	/* arrays */
	LtKind		elem_kind;
	Oid			elem_type;
	int16		elem_len;
	bool		elem_byval;
	char		elem_align;
	/* LT_IO, and the element of an array of LT_IO */
	FmgrInfo	out_fn;
	FmgrInfo	in_fn;
	Oid			ioparam;
	char		format[24];		/* Arrow's format string */
	char		child_format[24];	/* a list's element */
} LtColumn;

typedef struct LtLayout
{
	int			natts;
	LtColumn   *cols;			/* by attnum - 1; kind LT_NONE when dropped
								 * or not in the dataset */
	MemoryContext cxt;
} LtLayout;

extern LtLayout *lt_layout_build(Relation rel, MemoryContext cxt);
extern LtLayout *lt_layout_build_desc(TupleDesc desc, char **names, int nnames,
									  MemoryContext cxt);
extern LtLayout *lt_layout(Relation rel);
extern const char *lt_kind_name(LtKind kind);
extern bool lt_attribute_maps_alike(Form_pg_attribute a, Form_pg_attribute b);
extern bool lt_type_is_vector(Oid typid, int *dim, bool *half);
extern void lt_export_schema(LtLayout *layout, int ncols, const int *attidx,
							 struct ArrowSchema *out);
extern void lt_schema_set_metadata(struct ArrowSchema *s, const char *metadata, size_t len);
extern char *lt_arrow_to_pg_type(const struct ArrowSchema *field, char **comment);

/* encoding.c: Lance's encodings of a table's columns, from its options */
extern void lt_check_encoding_option(struct DefElem *d);
extern void lt_schema_set_encoding(struct ArrowSchema *schema, List *options,
								   bool every_column);
extern void lt_append_arrow_metadata(struct StringInfoData *buf, const char *metadata);
extern Datum lt_int128_to_numeric(const uint8 in[16], int precision, int scale);

/* A batch of rows being formed for a write */
typedef struct LtBuilder LtBuilder;

extern LtBuilder *lt_builder_create(LtLayout *layout);
extern void lt_builder_add(LtBuilder *b, TupleTableSlot *slot, uint64 rowid);
extern int64 lt_builder_rows(LtBuilder *b);
extern Size lt_builder_bytes(LtBuilder *b);
extern const uint64 *lt_builder_rowids(LtBuilder *b);
extern void lt_builder_export(LtBuilder *b, struct ArrowArray *array, struct ArrowSchema *schema);
extern void lt_builder_reset(LtBuilder *b);
extern void lt_builder_free(LtBuilder *b);

/* A batch read from the engine, and its columns' values as Datums */
typedef struct LtBatch
{
	struct ArrowArray array;
	struct ArrowSchema schema;
	bool		valid;
	int64		nrows;
	int			ncols;			/* the columns asked for */
	LtColumn  **cols;			/* each column's attribute, NULL for a
								 * column the dataset lacks */
	int			rowid_index;	/* the "_rowid" child, or -1 */
	int			distance_index; /* "_distance", or -1 */
	int			score_index;	/* "_score", or -1 */
} LtBatch;

extern void lt_batch_release(LtBatch *batch);
extern void lt_batch_bind(LtBatch *batch, int ncols, LtColumn **cols);
extern bool lt_batch_isnull(LtBatch *batch, int col, int64 row);
extern Datum lt_batch_datum(LtBatch *batch, int col, int64 row, bool *isnull);
extern uint64 lt_batch_rowid(LtBatch *batch, int64 row);
extern float4 lt_batch_float(LtBatch *batch, int child, int64 row, bool *isnull);

/*
 * scan.c: reading a table, for the access method's scans, ChScan's
 * LanceScan, vexec's source and fetches by TID (§2.8).
 */
typedef struct LtReadSpec
{
	List	   *attnums;		/* the attributes read; NIL: every one */
	char	   *filter;			/* a Lance SQL predicate, or NULL */
	int64		limit;			/* -1: none */
	bool		with_rowid;
	/* a nearest-neighbour search */
	AttrNumber	nn_attnum;		/* 0: none */
	float4	   *nn_query;
	int16	   *nn_query_half;
	int			nn_dim;
	int			nn_k;
	int			nn_metric;
	bool		prefilter;
	/* a full-text search */
	char	   *fts_query;
	AttrNumber	fts_attnum;
	/* the fragments a parallel participant reads, or NIL */
	List	   *fragments;
	/* the version read, 0: the snapshot's -- a parallel scan's, its first participant's */
	uint64		version;
} LtReadSpec;

typedef struct LtReader
{
	Relation	rel;
	LtTable    *table;
	LtLayout   *layout;
	Snapshot	snapshot;
	uint64		version;
	LeDataset  *dataset;
	LeScan	   *scan;
	LtBatch		batch;
	int64		row;			/* the next row of the batch */
	int			ncols;
	int		   *attidx;			/* each column's attnum - 1 */
	LtColumn  **cols;
	int			nmissing;		/* attributes the dataset has no column for */
	int		   *missing_attidx; /* whose rows PostgreSQL gives a value */
	bool		all_attrs;		/* every attribute of the relation read */
	MemoryContext rowcxt;		/* a row's values, reset at the next */
	uint64		rows_read;
	uint64		batches_read;
	bool		done;
	void	   *handles;		/* the engine handles, kept by a resource
								 * owner */
} LtReader;

extern LtReader *lt_reader_begin(Relation rel, Snapshot snapshot, LtReadSpec *spec);
extern bool lt_reader_next(LtReader *r, TupleTableSlot *slot);
extern bool lt_reader_next_batch(LtReader *r);
extern void lt_reader_end(LtReader *r);
extern void lt_reader_fill_spec(LtReader *r, LtReadSpec *spec, LeScanSpec *out,
								struct ArrowSchema *types);
extern LeDataset *lt_open_dataset(LtTable *t, uint64 version);
extern bool lt_fetch_rows(Relation rel, Snapshot snapshot, const uint64 *rowids, int n,
						  TupleTableSlot **slots, bool *found);
extern char *lt_reader_explain(Relation rel, Snapshot snapshot, LtReadSpec *spec, bool verbose);
extern void lt_scan_reset_readers(void);
extern void lt_store_batch_row(LtBatch *batch, int64 row, LtColumn **cols, const int *attidx,
							   int ncols, TupleTableSlot *slot);
extern void lt_reader_store_row(LtReader *r, LtBatch *batch, int64 row, TupleTableSlot *slot);

/*
 * xact.c: a transaction's state per table it writes (§2.7), and what
 * commit and abort do with it.
 */
typedef struct LtXactTable
{
	Oid			relid;			/* key */
	LeWrite    *write;
	LtBuilder  *builder;		/* rows not yet given to the engine */
	CommandId	builder_cid;
	SubTransactionId builder_subxid;
	uint64		base_version;
	bool		created;		/* the dataset was made in this transaction */
	SubTransactionId created_subxid;
	bool		dropped;		/* DROP TABLE in this transaction */
	SubTransactionId dropped_subxid;
	bool		published;		/* a version was committed to Lance */
	uint64		published_version;
	uint64		version_before;
	bool		serialize_writers;	/* unique indexes: one writer at a time */
	char	   *uri;
	char	   *storage_json;
	char	   *props;			/* the properties its versions carry */
	bool		managed;
	/* rows deleted, not yet handed to the engine: one command's */
	uint64	   *deletes;
	int			ndeletes;
	int			deletes_cap;
	CommandId	delete_cid;
	SubTransactionId delete_subxid;
	uint64		delete_version; /* the dataset version they were read at */
	/* the version a write's snapshot reads (lt_xact_read_version) */
	Snapshot	read_snapshot;
	TransactionId read_xmin;
	CommandId	read_curcid;
	uint64		read_completion;
	uint64		read_version;
	/* the rows the running command deleted */
	HTAB	   *cmd_deleted;
	CommandId	cmd_cid;
	/* the rows the transaction's changes add and take, for the planner */
	int64		rows_inserted;
	int64		rows_deleted;
	bool		truncated;		/* the version's rows are gone */
} LtXactTable;

extern void lt_xact_init(void);
extern LtXactTable *lt_xact_table(Relation rel, bool create);
extern LtXactTable *lt_xact_table_by_relid(Oid relid);
extern LeWrite *lt_xact_write(Relation rel, LtXactTable *xt);
extern void lt_xact_flush(LtXactTable *xt);
extern void lt_xact_flush_rel(Relation rel);
extern void lt_xact_insert(Relation rel, TupleTableSlot *slot, CommandId cid);
extern void lt_xact_insert_version(Relation rel, TupleTableSlot *slot, CommandId cid,
								   uint64 rowid);
extern void lt_xact_delete(Relation rel, uint64 rowid, CommandId cid, uint64 version);
extern uint64 lt_xact_read_version(Relation rel, Snapshot snapshot);
extern void lt_xact_note_created(Oid relid, const char *uri, const char *storage_json, bool managed);
extern void lt_xact_note_dropped(Oid relid, const char *uri, const char *storage_json, bool managed);
extern bool lt_xact_committing(void);
extern double lt_xact_rows(Oid relid, double committed);
extern void lt_xact_add_columns(Oid relid, List *attnums);
extern void lt_xact_drop_columns(Oid relid, List *attnums);
extern void lt_xact_truncate_committed(Relation rel);
extern int	lt_xact_row_state(Relation rel, uint64 rowid, CommandId cid, CommandId *dcid);
extern bool lt_xact_row_is_own(Oid relid, uint64 rowid);
extern char *lt_xact_properties(Oid relid);
extern uint64 lt_commit_base(Oid relid, const char *uri, const char *storage_json,
							 uint64 catalog_version, const char *props);

/* options.c: the tables' options, DDL (§2.4) */
extern void lt_options_init(void);
extern List *lt_pending_create_options(void);
extern bool lt_option_bool(List *options, const char *name, bool dflt);
extern char *lt_option_string(List *options, const char *name);
extern int64 lt_option_int(List *options, const char *name, int64 dflt);

/* customscan.c, filter.c: LanceScan (§2.8) */
extern void lt_customscan_init(void);
extern char *lt_filter_from_quals(Relation rel, LtTable *t, List *quals, List **pushed);

/*
 * A qual of LanceScan's that Lance can decide (filter.c): its predicate in
 * pieces -- Strings, and Integers that stand for the values the scan is
 * given when it starts: parameters, the outer row's values of a nested
 * loop, stable expressions of them -- and what the planner costs it by.
 */
typedef struct LtFilterClause
{
	List	   *pieces;
	List	   *values;			/* the value expressions, numbered from the
								 * first value lt_filter_clause() was given */
	List	   *value_info;		/* each one's, for lt_filter_value_append() */
	Bitmapset  *attnums;		/* the columns it compares */
	Bitmapset  *list_attnums;	/* those of them lists it compares by their
								 * elements: what a LABEL_LIST index answers */
	bool		sargable;		/* comparisons, IN lists, IS NULL, and ANDs
								 * and ORs of them: what a BTREE or BITMAP
								 * index answers, and a LABEL_LIST index of
								 * a list's elements */
	int			nsearch;		/* the index searches it takes */
} LtFilterClause;

typedef bool (*LtFilterValueFn) (struct StringInfoData *buf, int value, void *arg);

extern LtFilterClause *lt_filter_clause(PlannerInfo *root, Relation rel, LtTable *t,
										Index varno, Node *clause, Relids outer_relids,
										int first_value);
extern bool lt_filter_pieces_append(struct StringInfoData *buf, List *pieces,
									LtFilterValueFn fn, void *arg);
extern bool lt_filter_value_append(struct StringInfoData *buf, List *info, Datum value,
								   bool isnull, bool *never);

/* indexam.c: Lance's indices as PostgreSQL indexes (§2.9) */
extern bool lt_is_lance_index_am(Oid amoid);

/* vexec.c */
extern void lt_vexec_init(void);

#endif							/* LANCE_TAM_H */
