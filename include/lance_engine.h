/* SPDX-License-Identifier: Apache-2.0 */
/*-------------------------------------------------------------------------
 *
 * lance_engine.h
 *	  The C ABI of liblance_engine.so, the Rust library that links Lance
 *	  (docs/design.md §2.2).  The extension loads the library with dlopen()
 *	  in a backend as it first needs it, never in the postmaster, and reaches
 *	  every entry point through the table lance_engine_api() returns.
 *
 * Rules of the boundary:
 *
 *	- Every call is made on the backend's main thread and returns a status;
 *	  on failure *err holds a code and a message the caller frees with
 *	  free_string().  Nothing unwinds across the boundary: a panic inside the
 *	  library becomes LE_ERR_PANIC.
 *	- Lance's code never runs on the calling thread.  Each call hands its
 *	  work to the library's own threads, which block every signal, and waits;
 *	  while it waits it reads *cancel_flag and *die_flag (the server's
 *	  QueryCancelPending and ProcDiePending) and, once one is set, cancels
 *	  the work and returns LE_ERR_INTERRUPTED, so that the caller's
 *	  CHECK_FOR_INTERRUPTS() raises the cancel.  While the server holds
 *	  interrupts (*holdoff_count or *crit_section_count nonzero) the flags
 *	  are not read: a call made then runs to its end.  Other interrupts are
 *	  left for the caller to serve after the call.
 *	- The library never calls PostgreSQL.  Release callbacks of the arrays it
 *	  exports free only its own memory, and may run on any thread.
 *	- Batches cross as Arrow's C Data Interface: a struct array whose
 *	  children are the columns.  Ownership of an exported array passes to
 *	  the receiver, which calls its release callback when done.
 *
 *-------------------------------------------------------------------------
 */
#ifndef LANCE_ENGINE_H
#define LANCE_ENGINE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define LE_ABI_VERSION		2

/* Arrow's C Data Interface, as its specification defines it */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema
{
	const char *format;
	const char *name;
	const char *metadata;
	int64_t		flags;
	int64_t		n_children;
	struct ArrowSchema **children;
	struct ArrowSchema *dictionary;
	void		(*release) (struct ArrowSchema *);
	void	   *private_data;
};

struct ArrowArray
{
	int64_t		length;
	int64_t		null_count;
	int64_t		offset;
	int64_t		n_buffers;
	int64_t		n_children;
	const void **buffers;
	struct ArrowArray **children;
	struct ArrowArray *dictionary;
	void		(*release) (struct ArrowArray *);
	void	   *private_data;
};
#endif							/* ARROW_C_DATA_INTERFACE */

/* Status codes */
#define LE_OK					0
#define LE_ERR_INTERNAL			1	/* a fault of the library or of Lance */
#define LE_ERR_INVALID			2	/* the caller's input */
#define LE_ERR_NOT_FOUND		3	/* no dataset, version, index or column */
#define LE_ERR_CONFLICT			4	/* a commit lost to a concurrent one */
#define LE_ERR_INTERRUPTED		5	/* *interrupt_flag was set */
#define LE_ERR_IO				6	/* storage */
#define LE_ERR_PANIC			7
#define LE_ERR_UNSUPPORTED		8
#define LE_ERR_EXISTS			9	/* a dataset is already at the location */

typedef struct LeError
{
	int32_t		code;
	char	   *message;		/* free_string() */
} LeError;

/* Opaque handles */
typedef struct LeDataset LeDataset; /* a dataset at one version */
typedef struct LeScan LeScan;		/* a running scan */
typedef struct LeWrite LeWrite;		/* one transaction's changes to a table */

typedef struct LeConfig
{
	uint32_t	size;			/* sizeof(LeConfig) */
	int32_t		worker_threads; /* the library's runtime; 0: its default */
	volatile int32_t *cancel_flag;	/* nonzero: cancel the call */
	volatile int32_t *die_flag; /* nonzero: cancel the call */
	volatile uint32_t *holdoff_count;	/* nonzero: the flags are ignored */
	volatile uint32_t *crit_section_count;	/* nonzero: likewise */
	int64_t		index_cache_bytes;
	int64_t		metadata_cache_bytes;
	int32_t		poll_interval_us;	/* how often a wait reads the flag */
} LeConfig;

/* What le_dataset_info() reports, from the manifest alone */
typedef struct LeDatasetInfo
{
	uint64_t	version;
	uint64_t	num_rows;		/* live rows */
	uint64_t	num_deleted_rows;
	uint64_t	num_fragments;
	uint64_t	data_bytes;		/* the data files' sizes where recorded */
	uint64_t	max_fragment_id;
	uint64_t	max_row_id;		/* the highest row id any fragment holds, or 0 */
	bool		stable_row_ids;
	int64_t		timestamp_us;	/* when the version was committed */
} LeDatasetInfo;

/* Distance types of a nearest-neighbour search */
#define LE_METRIC_DEFAULT	0	/* the index's own, or L2 */
#define LE_METRIC_L2		1
#define LE_METRIC_COSINE	2
#define LE_METRIC_DOT		3
#define LE_METRIC_HAMMING	4

/* The element type of a query vector */
#define LE_VEC_FLOAT32		0
#define LE_VEC_FLOAT16		1
#define LE_VEC_FLOAT64		2
#define LE_VEC_UINT8		3

/*
 * A scan.  The batches hold the named columns in order, each as the type
 * "types" names for it (cast where the dataset's differs), then "_rowid"
 * (uint64) when with_row_id, then "_distance" (float32) for a
 * nearest-neighbour search and "_score" (float32) for a full-text one.
 *
 * With a LeWrite, the scan is its transaction's view: the rows it has
 * appended under a command id below cid are added, the rows it deleted under
 * one below cid are left out, and so are its rows of cid or later.
 */
typedef struct LeScanSpec
{
	uint32_t	size;			/* sizeof(LeScanSpec) */
	int32_t		ncolumns;
	const char *const *columns;
	struct ArrowSchema *types;	/* a struct schema, a child a column; NULL:
								 * the dataset's types.  Borrowed. */
	const char *filter;			/* a Lance SQL predicate, or NULL */
	int64_t		limit;			/* -1: no limit */
	int64_t		offset;
	bool		with_row_id;
	bool		scan_in_order;
	int32_t		batch_size;		/* 0: Lance's */
	int32_t		readahead;		/* batches decoded ahead of the caller */
	int32_t		nfragments;		/* -1: every fragment; else these */
	const uint64_t *fragments;
	/* nearest-neighbour search, when nn_column is not NULL */
	const char *nn_column;
	const void *nn_query;
	int32_t		nn_query_type;	/* LE_VEC_* */
	int32_t		nn_dim;
	int32_t		nn_k;
	int32_t		nn_metric;		/* LE_METRIC_* */
	int32_t		nn_nprobes;		/* 0: Lance's */
	int32_t		nn_ef;			/* 0: Lance's */
	int32_t		nn_refine;		/* 0: none */
	bool		nn_use_index;
	bool		prefilter;		/* the filter before the search, not after */
	/* full-text search, when fts_query is not NULL */
	const char *fts_column;		/* NULL: every indexed column */
	const char *fts_query;
	/* the transaction's own changes */
	LeWrite    *write;
	int64_t		cid;
} LeScanSpec;

/* How a transaction writes a table */
typedef struct LeWriteSpec
{
	uint32_t	size;			/* sizeof(LeWriteSpec) */
	bool		create;			/* no dataset yet: the commit makes it */
	bool		stable_row_ids; /* when creating */
	int32_t		max_rows_per_file;
	int32_t		max_rows_per_group;
	int64_t		buffer_rows;	/* rows held in memory before a flush */
	int64_t		buffer_bytes;
	const char *storage_version;	/* "stable", "2.1", ...; NULL: Lance's */
} LeWriteSpec;

/*
 * The table of entry points.  Every function returns LE_OK or an error
 * code, with *err filled on error, unless it says otherwise.
 */
typedef struct LeApi
{
	uint32_t	abi_version;
	uint32_t	size;			/* sizeof(LeApi) */
	const char *library_version;	/* the engine's and Lance's */

	int32_t		(*init) (const LeConfig *config, LeError *err);
	void		(*free_string) (char *s);

	/* datasets */
	int32_t		(*dataset_open) (const char *uri, const char *storage_options_json,
								 int64_t version, LeDataset **out, LeError *err);
	void		(*dataset_close) (LeDataset *ds);
	int32_t		(*dataset_info) (LeDataset *ds, LeDatasetInfo *out, LeError *err);
	int32_t		(*dataset_schema) (LeDataset *ds, struct ArrowSchema *out, LeError *err);
	int32_t		(*latest_version) (const char *uri, const char *storage_options_json,
								   uint64_t *out, LeError *err);
	/* versions newer than "after", as a JSON array of objects */
	int32_t		(*versions) (const char *uri, const char *storage_options_json,
							 uint64_t after, char **json_out, LeError *err);
	int32_t		(*indices) (LeDataset *ds, char **json_out, LeError *err);
	int32_t		(*fragments) (LeDataset *ds, char **json_out, LeError *err);
	/* the fragment ids of the version, and each one's live rows */
	int32_t		(*fragment_list) (LeDataset *ds, uint64_t **ids, uint64_t **rows,
								  int64_t *n, LeError *err);
	void		(*free_u64) (uint64_t *p, int64_t n);
	/*
	 * An empty dataset of the schema (borrowed), its rows carrying stable
	 * row ids; LE_ERR_EXISTS when one is at the location.
	 */
	int32_t		(*create_dataset) (const char *uri, const char *storage_options_json,
								   struct ArrowSchema *schema, const LeWriteSpec *spec,
								   const char *properties_json, uint64_t *version,
								   LeError *err);
	int32_t		(*restore) (const char *uri, const char *storage_options_json,
							uint64_t version, const char *properties_json,
							uint64_t *new_version, LeError *err);
	int32_t		(*remove_dataset) (const char *uri, const char *storage_options_json,
								   LeError *err);

	/* scans */
	int32_t		(*scan_begin) (LeDataset *ds, const LeScanSpec *spec, LeScan **out,
							   LeError *err);
	/* *done is set at the end, with no batch */
	int32_t		(*scan_next) (LeScan *scan, struct ArrowArray *array,
							  struct ArrowSchema *schema, bool *done, LeError *err);
	void		(*scan_end) (LeScan *scan);
	/* a scan's plan, for EXPLAIN */
	int32_t		(*scan_explain) (LeDataset *ds, const LeScanSpec *spec, bool verbose,
								 char **out, LeError *err);
	int32_t		(*count_rows) (LeDataset *ds, const LeScanSpec *spec, uint64_t *out,
							   LeError *err);

	/*
	 * Rows by row id, the transaction's own included: the batch holds the
	 * columns as for a scan, then "_rowid", for the ids found, in the order
	 * asked; an id no row has is left out.
	 */
	int32_t		(*take) (LeDataset *ds, const LeScanSpec *spec, const uint64_t *row_ids,
						 int64_t n, struct ArrowArray *array, struct ArrowSchema *schema,
						 LeError *err);

	/*
	 * Rows by their positions among the version's live rows, 0 the first,
	 * in the order asked, without "_rowid": ANALYZE's sample.  The
	 * transaction's own changes are not seen.
	 */
	int32_t		(*take_positions) (LeDataset *ds, const LeScanSpec *spec,
								   const uint64_t *positions, int64_t n,
								   struct ArrowArray *array, struct ArrowSchema *schema,
								   LeError *err);

	/* writes */
	int32_t		(*write_begin) (const char *uri, const char *storage_options_json,
								LeDataset *base, struct ArrowSchema *schema,
								const LeWriteSpec *spec, LeWrite **out, LeError *err);
	/*
	 * A batch of the table's schema; the array is moved, the schema
	 * borrowed.  A row id marked LE_ROWID_NEW_VERSION is a new version of
	 * that row, an in-place UPDATE's: the row keeps its id, the version its
	 * command and the commands after it see.
	 */
	int32_t		(*write_append) (LeWrite *w, struct ArrowArray *batch,
								 struct ArrowSchema *schema, const uint64_t *row_ids,
								 int64_t cid, uint32_t subxid, LeError *err);
	int32_t		(*write_delete) (LeWrite *w, const uint64_t *row_ids, int64_t n,
								 int64_t cid, uint32_t subxid, LeError *err);
	/* whether the transaction appended, or deleted, this row */
	int32_t		(*write_row_state) (LeWrite *w, uint64_t row_id, int64_t cid,
									int32_t *state, int64_t *state_cid, LeError *err);
	int32_t		(*write_flush) (LeWrite *w, LeError *err);
	int32_t		(*write_truncate) (LeWrite *w, int64_t cid, uint32_t subxid, LeError *err);
	int32_t		(*write_subxact) (LeWrite *w, uint32_t subxid, uint32_t parent,
								  bool commit, LeError *err);
	/*
	 * An index built now on "base", its files written, and committed with
	 * the transaction's changes; params_json holds the index's options.
	 */
	int32_t		(*write_create_index) (LeWrite *w, LeDataset *base, const char *column,
									   const char *index_type, const char *params_json,
									   const char *name, bool replace, uint32_t subxid,
									   LeError *err);
	int32_t		(*write_drop_index) (LeWrite *w, const char *name, uint32_t subxid,
									 LeError *err);
	bool		(*write_has_changes) (LeWrite *w);
	/*
	 * Commit the transaction's changes as new versions of the dataset, the
	 * last of which is *new_version; "properties" tag each version.  The
	 * changes are applied to "latest", the newest version the server's
	 * catalog has committed (NULL when the commit creates the dataset): a
	 * row the transaction deleted that "latest" no longer has, or has
	 * deleted, is a conflict, LE_ERR_CONFLICT.
	 */
	int32_t		(*write_commit) (LeWrite *w, LeDataset *latest, const char *properties_json,
								 uint64_t *new_version, LeError *err);
	/* forget the changes; with cleanup, remove the files they wrote */
	void		(*write_free) (LeWrite *w, bool cleanup);

	/* maintenance, committed at once */
	int32_t		(*optimize) (LeDataset *ds, const char *options_json,
							 const char *properties_json, uint64_t *new_version,
							 char **result_json, LeError *err);
	int32_t		(*cleanup) (LeDataset *ds, int64_t older_than_seconds,
							char **result_json, LeError *err);

	/*
	 * New columns, as one version on top of ds: fields a struct schema of
	 * them, values a one-row struct array of the value every row of ds
	 * takes, or NULL for nulls (the columns are then the manifest's alone).
	 */
	int32_t		(*add_columns) (LeDataset *ds, struct ArrowSchema *fields,
								struct ArrowArray *values, const char *properties_json,
								uint64_t *new_version, LeError *err);
	/* columns dropped, as one version on top of ds: the manifest's alone */
	int32_t		(*drop_columns) (LeDataset *ds, const char *const *names, int32_t n,
								 const char *properties_json, uint64_t *new_version,
								 LeError *err);

	/*
	 * Rows deleted by the transaction, as write_delete deletes them -- the
	 * version of each the command sees -- read at the dataset's version
	 * "version" (0: unknown): the commit deletes a committed row where that
	 * version had it, and a version another writer put under its id since
	 * is a conflict.
	 */
	int32_t		(*write_delete_read) (LeWrite *w, const uint64_t *row_ids, int64_t n,
									  uint64_t version, int64_t cid, uint32_t subxid,
									  LeError *err);
} LeApi;

/* write_append's mark on a row id: a new version of the row (bits 0..39) */
#define LE_ROWID_NEW_VERSION	(UINT64_C(1) << 63)

/* The library's one exported symbol */
typedef const LeApi *(*lance_engine_api_fn) (uint32_t abi_version);

#define LE_WRITE_ROW_NONE		0	/* not the transaction's */
#define LE_WRITE_ROW_APPENDED	1	/* appended by it, live */
#define LE_WRITE_ROW_DELETED	2	/* deleted by it */

#endif							/* LANCE_ENGINE_H */
