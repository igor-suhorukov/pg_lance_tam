/*-------------------------------------------------------------------------
 *
 * engine.c
 *	  Loading liblance_engine.so in a backend or worker on first use, never
 *	  in the postmaster, and raising its errors as PostgreSQL's
 *	  (docs/design.md §2.2).
 *
 *	  The library's threads block every signal and its calls wait on the
 *	  calling thread, reading QueryCancelPending and ProcDiePending; a call
 *	  they cut short returns LE_ERR_INTERRUPTED, and CHECK_FOR_INTERRUPTS()
 *	  then raises the interrupt once the call has returned.
 *
 * SPDX-License-Identifier: Apache-2.0
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dlfcn.h>

#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/elog.h"

#include "lance_tam.h"

static const LeApi *api = NULL;

const LeApi *
lt_api(void)
{
	char		path[MAXPGPATH];
	void	   *handle;
	lance_engine_api_fn entry;
	const LeApi *table;
	LeConfig	config;
	LeError		err = {0};

	if (api != NULL)
		return api;

	if (!IsUnderPostmaster && IsPostmasterEnvironment)
		elog(ERROR, "the Lance engine library is never loaded in the postmaster");

	if (lt_engine_library == NULL || lt_engine_library[0] == '\0' ||
		strcmp(lt_engine_library, "$libdir/liblance_engine.so") == 0)
		snprintf(path, sizeof(path), "%s/liblance_engine.so", pkglib_path);
	else
		strlcpy(path, lt_engine_library, sizeof(path));

	handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (handle == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FILE),
				 errmsg("could not load the Lance engine library \"%s\": %s", path, dlerror())));

	*(void **) &entry = dlsym(handle, "lance_engine_api");
	if (entry == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("the Lance engine library \"%s\" has no symbol lance_engine_api", path)));

	table = entry(LE_ABI_VERSION);
	if (table == NULL || table->abi_version != LE_ABI_VERSION || table->size < sizeof(LeApi))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the Lance engine library \"%s\" was built for another ABI", path),
				 errdetail("The extension expects ABI version %d.", LE_ABI_VERSION)));

	memset(&config, 0, sizeof(config));
	config.size = sizeof(config);
	config.worker_threads = lt_worker_threads;
	StaticAssertStmt(sizeof(QueryCancelPending) == sizeof(int32_t),
					 "sig_atomic_t is not 32 bits");
	config.cancel_flag = (volatile int32_t *) &QueryCancelPending;
	config.die_flag = (volatile int32_t *) &ProcDiePending;
	/* an abort's cleanup runs with interrupts held, and to its end */
	config.holdoff_count = (volatile uint32_t *) &InterruptHoldoffCount;
	config.crit_section_count = (volatile uint32_t *) &CritSectionCount;
	config.index_cache_bytes = (int64) lt_index_cache_mb * 1024 * 1024;
	config.metadata_cache_bytes = (int64) lt_metadata_cache_mb * 1024 * 1024;
	config.poll_interval_us = 5000;

	if (table->init(&config, &err) != LE_OK)
	{
		api = table;			/* so that lt_raise can free the message */
		PG_TRY();
		{
			lt_raise(&err, "initializing the Lance engine library");
		}
		PG_FINALLY();
		{
			api = NULL;
		}
		PG_END_TRY();
	}
	api = table;
	return api;
}

bool
lt_engine_loaded(void)
{
	return api != NULL;
}

/* A string the library allocated, copied into palloc'd memory and freed */
char *
lt_take_string(char *s)
{
	char	   *copy;

	if (s == NULL)
		return NULL;
	copy = pstrdup(s);
	if (api != NULL)
		api->free_string(s);
	return copy;
}

static int
sqlstate_of(int32 code)
{
	switch (code)
	{
		case LE_ERR_INVALID:
			return ERRCODE_INVALID_PARAMETER_VALUE;
		case LE_ERR_NOT_FOUND:
			return ERRCODE_UNDEFINED_OBJECT;
		case LE_ERR_CONFLICT:
			return ERRCODE_T_R_SERIALIZATION_FAILURE;
		case LE_ERR_INTERRUPTED:
			return ERRCODE_QUERY_CANCELED;
		case LE_ERR_IO:
			return ERRCODE_IO_ERROR;
		case LE_ERR_UNSUPPORTED:
			return ERRCODE_FEATURE_NOT_SUPPORTED;
		case LE_ERR_EXISTS:
			return ERRCODE_DUPLICATE_OBJECT;
		case LE_ERR_PANIC:
		case LE_ERR_INTERNAL:
		default:
			return ERRCODE_INTERNAL_ERROR;
	}
}

void
lt_raise(LeError *err, const char *context)
{
	char	   *message = lt_take_string(err->message);
	int32		code = err->code;

	err->message = NULL;
	if (code == LE_ERR_INTERRUPTED)
	{
		CHECK_FOR_INTERRUPTS();
		ereport(ERROR,
				(errcode(ERRCODE_QUERY_CANCELED),
				 errmsg("canceling statement due to user request"),
				 errcontext("%s", context)));
	}
	if (code == LE_ERR_CONFLICT)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("could not serialize access due to a concurrent change of the Lance dataset"),
				 errdetail("%s", message ? message : "a concurrent transaction committed first"),
				 errhint("The transaction might succeed if retried."),
				 errcontext("%s", context)));
	ereport(ERROR,
			(errcode(sqlstate_of(code)),
			 errmsg("%s", message ? message : "error in the Lance engine library"),
			 errcontext("%s", context)));
	pg_unreachable();
}

/* An error that must not raise: the cleanup after an abort */
void
lt_warn(LeError *err, const char *context)
{
	char	   *message = lt_take_string(err->message);

	err->message = NULL;
	ereport(WARNING,
			(errcode(sqlstate_of(err->code)),
			 errmsg("%s: %s", context, message ? message : "error in the Lance engine library")));
}
