/*-------------------------------------------------------------------------
 *
 * module.c
 *	  The extension's settings and hooks (docs/design.md §2.1).
 *
 *	  Loaded by shared_preload_libraries, the module registers vexec's
 *	  batch source and sink as well, and its hooks are in every backend
 *	  before its first statement: CREATE TABLE ... WITH (...) needs the
 *	  ProcessUtility hook.  Loaded on first use, the access method and its
 *	  default options work all the same.  The engine library is never loaded
 *	  here: a backend loads it when it first needs it (engine.c).
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <float.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"

#include "lance_tam.h"

PG_MODULE_MAGIC_EXT(
					.name = "lance_tam",
					.version = "0.1"
);

char	   *lt_engine_library = NULL;
char	   *lt_default_location = NULL;
char	   *lt_default_server = NULL;
int			lt_worker_threads = 4;
int			lt_index_cache_mb = 512;
int			lt_metadata_cache_mb = 128;
int			lt_batch_rows = 8192;
int			lt_readahead = 4;
int			lt_fetch_ahead = 1024;
int			lt_buffer_rows = 131072;
int			lt_nprobes = 0;
int			lt_ef = 0;
int			lt_refine_factor = 0;
bool		lt_enable_customscan = true;
bool		lt_enable_ann = true;
bool		lt_enable_filter_pushdown = true;
bool		lt_update_in_place = true;
bool		lt_scan_in_order = true;
bool		lt_vacuum_compact = true;
int			lt_version_retention = 3600;
int			lt_debug_fail_after_commit = 0;
int			lt_debug_vacuum_wait = 0;
bool		lt_preloaded = false;

void		_PG_init(void);

void
_PG_init(void)
{
	lt_preloaded = process_shared_preload_libraries_in_progress;

	DefineCustomStringVariable("lance.engine_library",
							   "The Lance engine library a backend loads.",
							   NULL, &lt_engine_library, "$libdir/liblance_engine.so",
							   PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("lance.default_location",
							   "Where a lance table's dataset is made when its options name no uri: a directory or an object store prefix (s3://bucket/prefix).",
							   "Empty: the directory pg_lance in the data directory.",
							   &lt_default_location, "", PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("lance.default_server",
							   "The storage server (of the foreign-data wrapper lance_storage) of a lance table whose options name none.",
							   NULL, &lt_default_server, "", PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.worker_threads",
							"The threads of a backend's Lance runtime, from the backend's first use of it.",
							NULL, &lt_worker_threads, 4, 1, 256, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.index_cache_mb",
							"The memory a backend's Lance session caches indices in.",
							NULL, &lt_index_cache_mb, 512, 0, INT_MAX / 2, PGC_SUSET,
							GUC_UNIT_MB, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.metadata_cache_mb",
							"The memory a backend's Lance session caches manifests and file metadata in.",
							NULL, &lt_metadata_cache_mb, 128, 0, INT_MAX / 2, PGC_SUSET,
							GUC_UNIT_MB, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.batch_rows",
							"Rows a batch of a scan or a write holds.",
							NULL, &lt_batch_rows, 8192, 64, 1048576, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.readahead",
							"Batches a scan decodes ahead of the executor.",
							NULL, &lt_readahead, 4, 1, 64, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.fetch_ahead",
							"Rows a fetch by TID takes ahead when a table's rows are fetched in the order of their TIDs, as AFTER triggers and foreign keys' checks fetch them.",
							"0: none; at most lance.batch_rows.",
							&lt_fetch_ahead, 1024, 0, 1048576, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.buffer_rows",
							"Rows a transaction's writes hold in memory before they are written as a fragment.",
							NULL, &lt_buffer_rows, 131072, 1024, INT_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.nprobes",
							"IVF partitions a vector search probes; 0: Lance's choice.",
							NULL, &lt_nprobes, 0, 0, 1000000, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.ef",
							"HNSW candidates a vector search keeps; 0: Lance's choice.",
							NULL, &lt_ef, 0, 0, 1000000, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.refine_factor",
							"Candidates a vector search reranks with exact distances, a multiple of k; 0: none.",
							NULL, &lt_refine_factor, 0, 0, 10000, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomRealVariable("lance.scan_start_cost",
							 "The planner's cost of starting a Lance scan, which each scan of a parameterized LanceScan pays.",
							 "A nested loop starts one for each outer row.",
							 &lt_scan_start_cost, 100.0, 0.0, DBL_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("lance.enable_customscan",
							 "Offer LanceScan, which reads only the columns a query needs and filters in Lance.",
							 NULL, &lt_enable_customscan, true, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("lance.enable_ann",
							 "Answer ORDER BY vector <-> q LIMIT k with Lance's nearest-neighbour search.",
							 NULL, &lt_enable_ann, true, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("lance.enable_filter_pushdown",
							 "Give a scan's quals to Lance as a filter.",
							 NULL, &lt_enable_filter_pushdown, true, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("lance.update_in_place",
							 "An UPDATE that changes no indexed column keeps the row's TID, so that no index needs an entry for it.",
							 "Off: every UPDATE gives the row a new TID, as before.",
							 &lt_update_in_place, true, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("lance.scan_in_order",
							 "Read a table's rows in the dataset's order, its fragments' and theirs.",
							 "Off lets Lance hand batches over as they are decoded.",
							 &lt_scan_in_order, true, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("lance.vacuum_compact",
							 "VACUUM compacts a lance table's dataset and optimizes its indices.",
							 NULL, &lt_vacuum_compact, true, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.version_retention",
							"VACUUM removes versions older than this; -1 keeps every version.",
							NULL, &lt_version_retention, 3600, -1, INT_MAX, PGC_SUSET,
							GUC_UNIT_S, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.debug_fail_after_commit",
							"Tests only: after a table's Lance commit, 1 raises an error, 2 aborts the process.",
							NULL, &lt_debug_fail_after_commit, 0, 0, 2, PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
	DefineCustomIntVariable("lance.debug_vacuum_wait",
							"Tests only: a VACUUM that removes index entries waits for this advisory lock once it has read the row ids; 0: none.",
							NULL, &lt_debug_vacuum_wait, 0, 0, INT_MAX, PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, NULL, NULL, NULL);
	MarkGUCPrefixReserved("lance");

	lt_options_init();
	lt_xact_init();
	lt_customscan_init();
	if (process_shared_preload_libraries_in_progress)
	{
		lt_rowids_init();
		lt_vexec_init();
	}
}
