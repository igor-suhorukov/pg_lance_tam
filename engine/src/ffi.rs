// SPDX-License-Identifier: Apache-2.0
//! The C ABI's structs, field for field as include/lance_engine.h declares
//! them, and the helpers every entry point uses.

use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};

use arrow::ffi::FFI_ArrowSchema;

use crate::error::{EResult, EngineError, LE_ERR_PANIC, LE_OK};
use crate::rt::panic_message;
use crate::write::LeWrite;

#[repr(C)]
pub struct LeError {
    pub code: i32,
    pub message: *mut c_char,
}

#[repr(C)]
pub struct LeConfig {
    pub size: u32,
    pub worker_threads: i32,
    pub cancel_flag: *const i32,
    pub die_flag: *const i32,
    pub holdoff_count: *const u32,
    pub crit_section_count: *const u32,
    pub index_cache_bytes: i64,
    pub metadata_cache_bytes: i64,
    pub poll_interval_us: i32,
}

#[repr(C)]
pub struct LeDatasetInfo {
    pub version: u64,
    pub num_rows: u64,
    pub num_deleted_rows: u64,
    pub num_fragments: u64,
    pub data_bytes: u64,
    pub max_fragment_id: u64,
    pub max_row_id: u64,
    pub stable_row_ids: bool,
    pub timestamp_us: i64,
}

#[repr(C)]
pub struct LeScanSpec {
    pub size: u32,
    pub ncolumns: i32,
    pub columns: *const *const c_char,
    pub types: *mut FFI_ArrowSchema,
    pub filter: *const c_char,
    pub limit: i64,
    pub offset: i64,
    pub with_row_id: bool,
    pub scan_in_order: bool,
    pub batch_size: i32,
    pub readahead: i32,
    pub nfragments: i32,
    pub fragments: *const u64,
    pub nn_column: *const c_char,
    pub nn_query: *const c_void,
    pub nn_query_type: i32,
    pub nn_dim: i32,
    pub nn_k: i32,
    pub nn_metric: i32,
    pub nn_nprobes: i32,
    pub nn_ef: i32,
    pub nn_refine: i32,
    pub nn_use_index: bool,
    pub prefilter: bool,
    pub fts_column: *const c_char,
    pub fts_query: *const c_char,
    pub write: *mut LeWrite,
    pub cid: i64,
}

#[repr(C)]
pub struct LeWriteSpec {
    pub size: u32,
    pub create: bool,
    pub stable_row_ids: bool,
    pub max_rows_per_file: i32,
    pub max_rows_per_group: i32,
    pub buffer_rows: i64,
    pub buffer_bytes: i64,
    pub storage_version: *const c_char,
}

pub const LE_METRIC_DEFAULT: i32 = 0;
pub const LE_METRIC_L2: i32 = 1;
pub const LE_METRIC_COSINE: i32 = 2;
pub const LE_METRIC_DOT: i32 = 3;
pub const LE_METRIC_HAMMING: i32 = 4;

pub const LE_VEC_FLOAT32: i32 = 0;
pub const LE_VEC_FLOAT16: i32 = 1;
pub const LE_VEC_FLOAT64: i32 = 2;
pub const LE_VEC_UINT8: i32 = 3;

pub const LE_WRITE_ROW_NONE: i32 = 0;
pub const LE_WRITE_ROW_APPENDED: i32 = 1;
pub const LE_WRITE_ROW_DELETED: i32 = 2;

/// A C string argument, or None for NULL.
///
/// # Safety
/// `p` is NULL or a NUL-terminated string that outlives the call.
pub unsafe fn opt_str<'a>(p: *const c_char) -> EResult<Option<&'a str>> {
    if p.is_null() {
        return Ok(None);
    }
    Ok(Some(unsafe { CStr::from_ptr(p) }.to_str()?))
}

/// # Safety
/// As [`opt_str`], and not NULL.
pub unsafe fn req_str<'a>(p: *const c_char, what: &str) -> EResult<&'a str> {
    unsafe { opt_str(p) }?.ok_or_else(|| EngineError::invalid(format!("{what} is NULL")))
}

/// Storage options as the extension passes them: a JSON object of strings.
pub fn parse_options(json: Option<&str>) -> EResult<HashMap<String, String>> {
    let Some(json) = json else {
        return Ok(HashMap::new());
    };
    if json.trim().is_empty() {
        return Ok(HashMap::new());
    }
    let value: serde_json::Value = serde_json::from_str(json)?;
    let obj = value
        .as_object()
        .ok_or_else(|| EngineError::invalid("storage options must be a JSON object"))?;
    let mut out = HashMap::new();
    for (k, v) in obj {
        let s = match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Null => continue,
            other => other.to_string(),
        };
        out.insert(k.clone(), s);
    }
    Ok(out)
}

pub fn into_c_string(s: String) -> *mut c_char {
    // An interior NUL cannot be passed to C: replace it.
    let s = if s.contains('\0') {
        s.replace('\0', " ")
    } else {
        s
    };
    CString::new(s).map(CString::into_raw).unwrap_or(std::ptr::null_mut())
}

/// # Safety
/// `err` is NULL or points at a LeError.
pub unsafe fn set_error(err: *mut LeError, e: EngineError) -> i32 {
    if !err.is_null() {
        unsafe {
            (*err).code = e.code;
            (*err).message = into_c_string(e.message);
        }
    }
    e.code
}

/// The body of an entry point: its result as a status, a panic caught.
///
/// # Safety
/// `err` is NULL or points at a LeError.
pub unsafe fn guard<F>(err: *mut LeError, f: F) -> i32
where
    F: FnOnce() -> EResult<()>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => LE_OK,
        Ok(Err(e)) => unsafe { set_error(err, e) },
        Err(panic) => unsafe {
            set_error(
                err,
                EngineError::new(
                    LE_ERR_PANIC,
                    format!("panic in the engine library: {}", panic_message(panic)),
                ),
            )
        },
    }
}

/// A void entry point's body, a panic swallowed: the caller cannot be told.
pub fn guard_void<F: FnOnce()>(f: F) {
    let _ = catch_unwind(AssertUnwindSafe(f));
}
