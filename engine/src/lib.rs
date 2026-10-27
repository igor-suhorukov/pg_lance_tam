// SPDX-License-Identifier: Apache-2.0
//! liblance_engine.so: Lance behind the C ABI of include/lance_engine.h
//! (docs/design.md §2.2).  The one exported symbol is lance_engine_api(),
//! the table of entry points; every entry point catches what Lance raises
//! or panics with and returns it as a status.

mod columns;
mod dataset;
mod error;
mod ffi;
mod maint;
mod rt;
mod scan;
mod types;
mod write;

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use crate::dataset::LeDataset;
use crate::error::{EngineError, LE_ERR_INVALID};
use crate::ffi::*;
use crate::rt::run;
use crate::scan::{LeScan, ScanSpec};
use crate::write::{LeWrite, RowState, Spec};

const ABI_VERSION: u32 = 2;

static LIBRARY_VERSION: LazyLock<CString> = LazyLock::new(|| {
    CString::new(format!(
        "lance_engine {} (lance {})",
        env!("CARGO_PKG_VERSION"),
        LANCE_VERSION
    ))
    .unwrap()
});

const LANCE_VERSION: &str = "14.0.0-beta.19";

fn dataset_ref<'a>(ds: *mut LeDataset) -> Result<&'a LeDataset, EngineError> {
    if ds.is_null() {
        return Err(EngineError::invalid("the dataset handle is NULL"));
    }
    Ok(unsafe { &*ds })
}

fn write_ref<'a>(w: *mut LeWrite) -> Result<&'a LeWrite, EngineError> {
    if w.is_null() {
        return Err(EngineError::invalid("the write handle is NULL"));
    }
    Ok(unsafe { &*w })
}

unsafe extern "C" fn le_init(config: *const LeConfig, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            if config.is_null() {
                return Err(EngineError::invalid("the configuration is NULL"));
            }
            let c = &*config;
            if (c.size as usize) < std::mem::size_of::<LeConfig>() {
                return Err(EngineError::invalid("the configuration is from an older ABI"));
            }
            rt::init(rt::Config {
                worker_threads: c.worker_threads.max(0) as usize,
                cancel_flag: c.cancel_flag,
                die_flag: c.die_flag,
                holdoff_count: c.holdoff_count,
                crit_section_count: c.crit_section_count,
                index_cache_bytes: c.index_cache_bytes.max(0) as usize,
                metadata_cache_bytes: c.metadata_cache_bytes.max(0) as usize,
                poll: Duration::from_micros(c.poll_interval_us.clamp(100, 1_000_000) as u64),
            })
        })
    }
}

unsafe extern "C" fn le_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

unsafe fn options_arg(json: *const c_char) -> Result<Arc<HashMap<String, String>>, EngineError> {
    Ok(Arc::new(parse_options(unsafe { opt_str(json) }?)?))
}

unsafe extern "C" fn le_dataset_open(
    uri: *const c_char,
    options: *const c_char,
    version: i64,
    out: *mut *mut LeDataset,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let uri = req_str(uri, "the uri")?.to_string();
            let opts = options_arg(options)?;
            let v = (version >= 0).then_some(version as u64);
            let ds = run(async move { dataset::open(&uri, &opts, v).await })?;
            *out = Box::into_raw(Box::new(LeDataset::new(ds)));
            Ok(())
        })
    }
}

unsafe extern "C" fn le_dataset_close(ds: *mut LeDataset) {
    if !ds.is_null() {
        guard_void(|| drop(unsafe { Box::from_raw(ds) }));
    }
}

unsafe extern "C" fn le_dataset_info(ds: *mut LeDataset, out: *mut LeDatasetInfo, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?;
            let i = dataset::info(&d.ds);
            let handle = d.ds.clone();
            let max_row_id = run(async move { dataset::max_row_id(&handle).await })?;
            *out = LeDatasetInfo {
                version: i.version,
                num_rows: i.num_rows,
                num_deleted_rows: i.num_deleted_rows,
                num_fragments: i.num_fragments,
                data_bytes: i.data_bytes,
                max_fragment_id: i.max_fragment_id,
                max_row_id: max_row_id.max(i.max_row_id),
                stable_row_ids: i.stable_row_ids,
                timestamp_us: i.timestamp_us,
            };
            Ok(())
        })
    }
}

unsafe extern "C" fn le_dataset_schema(ds: *mut LeDataset, out: *mut FFI_ArrowSchema, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?;
            let schema = dataset::arrow_schema(&d.ds);
            let ffi = FFI_ArrowSchema::try_from(&schema)?;
            std::ptr::write(out, ffi);
            Ok(())
        })
    }
}

unsafe extern "C" fn le_latest_version(
    uri: *const c_char,
    options: *const c_char,
    out: *mut u64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let uri = req_str(uri, "the uri")?.to_string();
            let opts = options_arg(options)?;
            let v = run(async move {
                let ds = dataset::open(&uri, &opts, None).await?;
                Ok(ds.latest_version_id().await?)
            })?;
            *out = v;
            Ok(())
        })
    }
}

unsafe extern "C" fn le_versions(
    uri: *const c_char,
    options: *const c_char,
    after: u64,
    out: *mut *mut c_char,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let uri = req_str(uri, "the uri")?.to_string();
            let opts = options_arg(options)?;
            let json = run(async move {
                let ds = dataset::open(&uri, &opts, None).await?;
                dataset::versions_json(ds, after).await
            })?;
            *out = into_c_string(json);
            Ok(())
        })
    }
}

unsafe extern "C" fn le_indices(ds: *mut LeDataset, out: *mut *mut c_char, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let json = run(async move { dataset::indices_json(d).await })?;
            *out = into_c_string(json);
            Ok(())
        })
    }
}

unsafe extern "C" fn le_fragments(ds: *mut LeDataset, out: *mut *mut c_char, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?;
            *out = into_c_string(dataset::fragments_json(&d.ds)?);
            Ok(())
        })
    }
}

fn leak_u64(v: Vec<u64>) -> *mut u64 {
    let boxed = v.into_boxed_slice();
    Box::into_raw(boxed) as *mut u64
}

unsafe extern "C" fn le_fragment_list(
    ds: *mut LeDataset,
    ids: *mut *mut u64,
    rows: *mut *mut u64,
    n: *mut i64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?;
            let (i, r) = dataset::fragment_list(&d.ds);
            *n = i.len() as i64;
            *ids = leak_u64(i);
            *rows = leak_u64(r);
            Ok(())
        })
    }
}

unsafe extern "C" fn le_free_u64(p: *mut u64, n: i64) {
    if !p.is_null() {
        let slice = std::ptr::slice_from_raw_parts_mut(p, n.max(0) as usize);
        drop(unsafe { Box::from_raw(slice) });
    }
}

unsafe extern "C" fn le_create_dataset(
    uri: *const c_char,
    options: *const c_char,
    schema: *mut FFI_ArrowSchema,
    spec: *const LeWriteSpec,
    properties: *const c_char,
    version: *mut u64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let uri = req_str(uri, "the uri")?.to_string();
            let opts = options_arg(options)?;
            if schema.is_null() {
                return Err(EngineError::invalid("the schema is NULL"));
            }
            let schema = arrow_schema::Schema::try_from(&*schema)?;
            let storage_version = if spec.is_null() {
                None
            } else {
                match opt_str((*spec).storage_version)? {
                    None => None,
                    Some(v) => Some(
                        v.parse::<lance_file::version::LanceFileVersion>().map_err(|e| {
                            EngineError::new(LE_ERR_INVALID, format!("storage version \"{v}\": {e}"))
                        })?,
                    ),
                }
            };
            let props = parse_options(opt_str(properties)?)?;
            *version = run(async move { dataset::create(&uri, &opts, schema, storage_version, props).await })?;
            Ok(())
        })
    }
}

unsafe extern "C" fn le_restore(
    uri: *const c_char,
    options: *const c_char,
    version: u64,
    properties: *const c_char,
    new_version: *mut u64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let uri = req_str(uri, "the uri")?.to_string();
            let opts = options_arg(options)?;
            let props = parse_options(opt_str(properties)?)?;
            let v = run(async move { dataset::restore(&uri, &opts, version, props).await })?;
            *new_version = v;
            Ok(())
        })
    }
}

unsafe extern "C" fn le_remove_dataset(uri: *const c_char, options: *const c_char, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            let uri = req_str(uri, "the uri")?.to_string();
            let opts = options_arg(options)?;
            run(async move { dataset::remove(&uri, &opts).await })
        })
    }
}

unsafe extern "C" fn le_scan_begin(
    ds: *mut LeDataset,
    spec: *const LeScanSpec,
    out: *mut *mut LeScan,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let spec = ScanSpec::from_c(spec)?;
            let scan = scan::begin(d, spec)?;
            *out = Box::into_raw(Box::new(scan));
            Ok(())
        })
    }
}

unsafe extern "C" fn le_scan_next(
    scan: *mut LeScan,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    done: *mut bool,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            if scan.is_null() {
                return Err(EngineError::invalid("the scan handle is NULL"));
            }
            match (*scan).next()? {
                Some(batch) => {
                    *done = false;
                    types::export_batch(batch, array, schema)
                }
                None => {
                    *done = true;
                    Ok(())
                }
            }
        })
    }
}

unsafe extern "C" fn le_scan_end(scan: *mut LeScan) {
    if !scan.is_null() {
        guard_void(|| drop(unsafe { Box::from_raw(scan) }));
    }
}

unsafe extern "C" fn le_scan_explain(
    ds: *mut LeDataset,
    spec: *const LeScanSpec,
    verbose: bool,
    out: *mut *mut c_char,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let spec = ScanSpec::from_c(spec)?;
            let text = run(async move { scan::explain(d, spec, verbose).await })?;
            *out = into_c_string(text);
            Ok(())
        })
    }
}

unsafe extern "C" fn le_count_rows(
    ds: *mut LeDataset,
    spec: *const LeScanSpec,
    out: *mut u64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let spec = ScanSpec::from_c(spec)?;
            *out = run(async move { scan::count_rows(d, spec).await })?;
            Ok(())
        })
    }
}

unsafe extern "C" fn le_take(
    ds: *mut LeDataset,
    spec: *const LeScanSpec,
    row_ids: *const u64,
    n: i64,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let spec = ScanSpec::from_c(spec)?;
            let ids = if n > 0 {
                std::slice::from_raw_parts(row_ids, n as usize).to_vec()
            } else {
                Vec::new()
            };
            let batch = run(async move { scan::take(d, spec, ids).await })?;
            types::export_batch(batch, array, schema)
        })
    }
}

unsafe extern "C" fn le_take_positions(
    ds: *mut LeDataset,
    spec: *const LeScanSpec,
    positions: *const u64,
    n: i64,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let spec = ScanSpec::from_c(spec)?;
            let p = if n > 0 {
                std::slice::from_raw_parts(positions, n as usize).to_vec()
            } else {
                Vec::new()
            };
            let batch = run(async move { scan::take_positions(d, spec, p).await })?;
            types::export_batch(batch, array, schema)
        })
    }
}

unsafe extern "C" fn le_write_begin(
    uri: *const c_char,
    options: *const c_char,
    base: *mut LeDataset,
    _schema: *mut FFI_ArrowSchema,
    spec: *const LeWriteSpec,
    out: *mut *mut LeWrite,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let uri = req_str(uri, "the uri")?.to_string();
            let opts = options_arg(options)?;
            if spec.is_null() {
                return Err(EngineError::invalid("the write spec is NULL"));
            }
            let s = &*spec;
            if (s.size as usize) < std::mem::size_of::<LeWriteSpec>() {
                return Err(EngineError::invalid("the write spec is from an older ABI"));
            }
            let storage_version = match opt_str(s.storage_version)? {
                None => None,
                Some(v) => Some(
                    v.parse::<lance_file::version::LanceFileVersion>()
                        .map_err(|e| EngineError::new(LE_ERR_INVALID, format!("storage version \"{v}\": {e}")))?,
                ),
            };
            let spec = Spec {
                max_rows_per_file: if s.max_rows_per_file > 0 { s.max_rows_per_file as usize } else { 1024 * 1024 },
                max_rows_per_group: if s.max_rows_per_group > 0 { s.max_rows_per_group as usize } else { 1024 },
                buffer_rows: if s.buffer_rows > 0 { s.buffer_rows as usize } else { 128 * 1024 },
                buffer_bytes: if s.buffer_bytes > 0 { s.buffer_bytes as usize } else { 64 << 20 },
                storage_version,
            };
            let base = if base.is_null() { None } else { Some((*base).ds.clone()) };
            let state = run(async move { write::begin(&uri, opts, base, spec).await })?;
            *out = Box::into_raw(Box::new(LeWrite {
                inner: Arc::new(tokio::sync::Mutex::new(state)),
            }));
            Ok(())
        })
    }
}

unsafe extern "C" fn le_write_append(
    w: *mut LeWrite,
    batch: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    row_ids: *const u64,
    cid: i64,
    subxid: u32,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            let batch = types::import_batch(batch, schema)?;
            let ids = if batch.num_rows() > 0 {
                std::slice::from_raw_parts(row_ids, batch.num_rows()).to_vec()
            } else {
                Vec::new()
            };
            run(async move { inner.lock().await.append(batch, ids, cid, subxid).await })
        })
    }
}

unsafe extern "C" fn le_write_delete(
    w: *mut LeWrite,
    row_ids: *const u64,
    n: i64,
    cid: i64,
    subxid: u32,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            let ids = if n > 0 {
                std::slice::from_raw_parts(row_ids, n as usize).to_vec()
            } else {
                return Ok(());
            };
            run(async move {
                inner.lock().await.delete(&ids, 0, cid, subxid);
                Ok(())
            })
        })
    }
}

unsafe extern "C" fn le_write_delete_read(
    w: *mut LeWrite,
    row_ids: *const u64,
    n: i64,
    version: u64,
    cid: i64,
    subxid: u32,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            let ids = if n > 0 {
                std::slice::from_raw_parts(row_ids, n as usize).to_vec()
            } else {
                return Ok(());
            };
            run(async move {
                inner.lock().await.delete(&ids, version, cid, subxid);
                Ok(())
            })
        })
    }
}

unsafe extern "C" fn le_write_row_state(
    w: *mut LeWrite,
    row_id: u64,
    cid: i64,
    state: *mut i32,
    state_cid: *mut i64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            let s = run(async move { Ok(inner.lock().await.row_state(row_id, cid)) })?;
            let (st, c) = match s {
                RowState::None => (LE_WRITE_ROW_NONE, -1),
                RowState::Appended => (LE_WRITE_ROW_APPENDED, -1),
                RowState::Deleted(c) => (LE_WRITE_ROW_DELETED, c),
            };
            *state = st;
            *state_cid = c;
            Ok(())
        })
    }
}

unsafe extern "C" fn le_write_flush(w: *mut LeWrite, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            run(async move { inner.lock().await.flush().await })
        })
    }
}

unsafe extern "C" fn le_write_truncate(w: *mut LeWrite, cid: i64, subxid: u32, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            run(async move {
                inner.lock().await.truncate(cid, subxid);
                Ok(())
            })
        })
    }
}

unsafe extern "C" fn le_write_subxact(
    w: *mut LeWrite,
    subxid: u32,
    parent: u32,
    commit: bool,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            run(async move {
                inner.lock().await.subxact(subxid, parent, commit);
                Ok(())
            })
        })
    }
}

#[allow(clippy::too_many_arguments)]
unsafe extern "C" fn le_write_create_index(
    w: *mut LeWrite,
    base: *mut LeDataset,
    column: *const c_char,
    index_type: *const c_char,
    params_json: *const c_char,
    name: *const c_char,
    replace: bool,
    subxid: u32,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            let base = dataset_ref(base)?.ds.clone();
            let column = req_str(column, "the column")?.to_string();
            let index_type = req_str(index_type, "the index type")?.to_string();
            let params = opt_str(params_json)?.map(str::to_string);
            let name = opt_str(name)?.map(str::to_string);
            run(async move {
                inner
                    .lock()
                    .await
                    .create_index(base, &column, &index_type, params.as_deref(), name.as_deref(), replace, subxid)
                    .await
            })
        })
    }
}

unsafe extern "C" fn le_write_drop_index(w: *mut LeWrite, name: *const c_char, subxid: u32, err: *mut LeError) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            let name = req_str(name, "the index name")?.to_string();
            run(async move {
                inner.lock().await.drop_index(&name, subxid);
                Ok(())
            })
        })
    }
}

unsafe extern "C" fn le_write_has_changes(w: *mut LeWrite) -> bool {
    let Ok(lw) = write_ref(w) else {
        return false;
    };
    let inner = lw.inner.clone();
    run(async move { Ok(inner.lock().await.has_changes()) }).unwrap_or(true)
}

unsafe extern "C" fn le_write_commit(
    w: *mut LeWrite,
    latest: *mut LeDataset,
    properties: *const c_char,
    new_version: *mut u64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let inner = write_ref(w)?.inner.clone();
            let props = parse_options(opt_str(properties)?)?;
            let latest = match latest.is_null() {
                true => None,
                false => Some((*latest).ds.clone()),
            };
            let v = run(async move {
                let mut state = inner.lock().await;
                let latest = match latest {
                    Some(l) => l,
                    None => {
                        let (uri, opts) = (state.uri.clone(), state.options.clone());
                        dataset::open(&uri, &opts, None).await?
                    }
                };
                state.commit(latest, props).await
            })?;
            *new_version = v;
            Ok(())
        })
    }
}

unsafe extern "C" fn le_write_free(w: *mut LeWrite, cleanup: bool) {
    if w.is_null() {
        return;
    }
    guard_void(|| {
        let lw = unsafe { Box::from_raw(w) };
        if cleanup {
            let inner = lw.inner.clone();
            let _ = run(async move {
                inner.lock().await.cleanup().await;
                Ok(())
            });
        }
    });
}

unsafe extern "C" fn le_optimize(
    ds: *mut LeDataset,
    options: *const c_char,
    _properties: *const c_char,
    new_version: *mut u64,
    result: *mut *mut c_char,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let opts = opt_str(options)?.map(str::to_string);
            let (v, json) = run(async move { maint::optimize(d, opts.as_deref()).await })?;
            *new_version = v;
            if !result.is_null() {
                *result = into_c_string(json);
            }
            Ok(())
        })
    }
}

unsafe extern "C" fn le_add_columns(
    ds: *mut LeDataset,
    fields: *mut FFI_ArrowSchema,
    values: *mut FFI_ArrowArray,
    properties: *const c_char,
    new_version: *mut u64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            if fields.is_null() {
                return Err(EngineError::invalid("the new columns' schema is NULL"));
            }
            let schema = arrow_schema::Schema::try_from(&*fields)?;
            let values = if values.is_null() {
                None
            } else {
                Some(types::import_batch(values, fields)?)
            };
            let props = parse_options(opt_str(properties)?)?;
            *new_version = run(async move { columns::add_columns(d, schema, values, props).await })?;
            Ok(())
        })
    }
}

unsafe extern "C" fn le_drop_columns(
    ds: *mut LeDataset,
    names: *const *const c_char,
    n: i32,
    properties: *const c_char,
    new_version: *mut u64,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let mut columns = Vec::new();
            for i in 0..n.max(0) as usize {
                columns.push(req_str(*names.add(i), "a column's name")?.to_string());
            }
            let props = parse_options(opt_str(properties)?)?;
            *new_version = run(async move { columns::drop_columns(d, columns, props).await })?;
            Ok(())
        })
    }
}

unsafe extern "C" fn le_cleanup(
    ds: *mut LeDataset,
    older_than_seconds: i64,
    result: *mut *mut c_char,
    err: *mut LeError,
) -> i32 {
    unsafe {
        guard(err, || {
            let d = dataset_ref(ds)?.ds.clone();
            let json = run(async move { maint::cleanup(d, older_than_seconds).await })?;
            if !result.is_null() {
                *result = into_c_string(json);
            }
            Ok(())
        })
    }
}

#[repr(C)]
pub struct LeApi {
    abi_version: u32,
    size: u32,
    library_version: *const c_char,
    init: unsafe extern "C" fn(*const LeConfig, *mut LeError) -> i32,
    free_string: unsafe extern "C" fn(*mut c_char),
    dataset_open: unsafe extern "C" fn(*const c_char, *const c_char, i64, *mut *mut LeDataset, *mut LeError) -> i32,
    dataset_close: unsafe extern "C" fn(*mut LeDataset),
    dataset_info: unsafe extern "C" fn(*mut LeDataset, *mut LeDatasetInfo, *mut LeError) -> i32,
    dataset_schema: unsafe extern "C" fn(*mut LeDataset, *mut FFI_ArrowSchema, *mut LeError) -> i32,
    latest_version: unsafe extern "C" fn(*const c_char, *const c_char, *mut u64, *mut LeError) -> i32,
    versions: unsafe extern "C" fn(*const c_char, *const c_char, u64, *mut *mut c_char, *mut LeError) -> i32,
    indices: unsafe extern "C" fn(*mut LeDataset, *mut *mut c_char, *mut LeError) -> i32,
    fragments: unsafe extern "C" fn(*mut LeDataset, *mut *mut c_char, *mut LeError) -> i32,
    fragment_list: unsafe extern "C" fn(*mut LeDataset, *mut *mut u64, *mut *mut u64, *mut i64, *mut LeError) -> i32,
    free_u64: unsafe extern "C" fn(*mut u64, i64),
    create_dataset: unsafe extern "C" fn(
        *const c_char,
        *const c_char,
        *mut FFI_ArrowSchema,
        *const LeWriteSpec,
        *const c_char,
        *mut u64,
        *mut LeError,
    ) -> i32,
    restore: unsafe extern "C" fn(*const c_char, *const c_char, u64, *const c_char, *mut u64, *mut LeError) -> i32,
    remove_dataset: unsafe extern "C" fn(*const c_char, *const c_char, *mut LeError) -> i32,
    scan_begin: unsafe extern "C" fn(*mut LeDataset, *const LeScanSpec, *mut *mut LeScan, *mut LeError) -> i32,
    scan_next:
        unsafe extern "C" fn(*mut LeScan, *mut FFI_ArrowArray, *mut FFI_ArrowSchema, *mut bool, *mut LeError) -> i32,
    scan_end: unsafe extern "C" fn(*mut LeScan),
    scan_explain: unsafe extern "C" fn(*mut LeDataset, *const LeScanSpec, bool, *mut *mut c_char, *mut LeError) -> i32,
    count_rows: unsafe extern "C" fn(*mut LeDataset, *const LeScanSpec, *mut u64, *mut LeError) -> i32,
    take: unsafe extern "C" fn(
        *mut LeDataset,
        *const LeScanSpec,
        *const u64,
        i64,
        *mut FFI_ArrowArray,
        *mut FFI_ArrowSchema,
        *mut LeError,
    ) -> i32,
    take_positions: unsafe extern "C" fn(
        *mut LeDataset,
        *const LeScanSpec,
        *const u64,
        i64,
        *mut FFI_ArrowArray,
        *mut FFI_ArrowSchema,
        *mut LeError,
    ) -> i32,
    write_begin: unsafe extern "C" fn(
        *const c_char,
        *const c_char,
        *mut LeDataset,
        *mut FFI_ArrowSchema,
        *const LeWriteSpec,
        *mut *mut LeWrite,
        *mut LeError,
    ) -> i32,
    write_append:
        unsafe extern "C" fn(*mut LeWrite, *mut FFI_ArrowArray, *mut FFI_ArrowSchema, *const u64, i64, u32, *mut LeError) -> i32,
    write_delete: unsafe extern "C" fn(*mut LeWrite, *const u64, i64, i64, u32, *mut LeError) -> i32,
    write_row_state: unsafe extern "C" fn(*mut LeWrite, u64, i64, *mut i32, *mut i64, *mut LeError) -> i32,
    write_flush: unsafe extern "C" fn(*mut LeWrite, *mut LeError) -> i32,
    write_truncate: unsafe extern "C" fn(*mut LeWrite, i64, u32, *mut LeError) -> i32,
    write_subxact: unsafe extern "C" fn(*mut LeWrite, u32, u32, bool, *mut LeError) -> i32,
    write_create_index: unsafe extern "C" fn(
        *mut LeWrite,
        *mut LeDataset,
        *const c_char,
        *const c_char,
        *const c_char,
        *const c_char,
        bool,
        u32,
        *mut LeError,
    ) -> i32,
    write_drop_index: unsafe extern "C" fn(*mut LeWrite, *const c_char, u32, *mut LeError) -> i32,
    write_has_changes: unsafe extern "C" fn(*mut LeWrite) -> bool,
    write_commit: unsafe extern "C" fn(*mut LeWrite, *mut LeDataset, *const c_char, *mut u64, *mut LeError) -> i32,
    write_free: unsafe extern "C" fn(*mut LeWrite, bool),
    optimize: unsafe extern "C" fn(
        *mut LeDataset,
        *const c_char,
        *const c_char,
        *mut u64,
        *mut *mut c_char,
        *mut LeError,
    ) -> i32,
    cleanup: unsafe extern "C" fn(*mut LeDataset, i64, *mut *mut c_char, *mut LeError) -> i32,
    add_columns: unsafe extern "C" fn(
        *mut LeDataset,
        *mut FFI_ArrowSchema,
        *mut FFI_ArrowArray,
        *const c_char,
        *mut u64,
        *mut LeError,
    ) -> i32,
    drop_columns: unsafe extern "C" fn(
        *mut LeDataset,
        *const *const c_char,
        i32,
        *const c_char,
        *mut u64,
        *mut LeError,
    ) -> i32,
    write_delete_read:
        unsafe extern "C" fn(*mut LeWrite, *const u64, i64, u64, i64, u32, *mut LeError) -> i32,
}

// SAFETY: the table holds function pointers and a pointer to a static string.
unsafe impl Sync for LeApi {}
unsafe impl Send for LeApi {}

static API: LazyLock<LeApi> = LazyLock::new(|| LeApi {
    abi_version: ABI_VERSION,
    size: std::mem::size_of::<LeApi>() as u32,
    library_version: LIBRARY_VERSION.as_ptr(),
    init: le_init,
    free_string: le_free_string,
    dataset_open: le_dataset_open,
    dataset_close: le_dataset_close,
    dataset_info: le_dataset_info,
    dataset_schema: le_dataset_schema,
    latest_version: le_latest_version,
    versions: le_versions,
    indices: le_indices,
    fragments: le_fragments,
    fragment_list: le_fragment_list,
    free_u64: le_free_u64,
    create_dataset: le_create_dataset,
    restore: le_restore,
    remove_dataset: le_remove_dataset,
    scan_begin: le_scan_begin,
    scan_next: le_scan_next,
    scan_end: le_scan_end,
    scan_explain: le_scan_explain,
    count_rows: le_count_rows,
    take: le_take,
    take_positions: le_take_positions,
    write_begin: le_write_begin,
    write_append: le_write_append,
    write_delete: le_write_delete,
    write_row_state: le_write_row_state,
    write_flush: le_write_flush,
    write_truncate: le_write_truncate,
    write_subxact: le_write_subxact,
    write_create_index: le_write_create_index,
    write_drop_index: le_write_drop_index,
    write_has_changes: le_write_has_changes,
    write_commit: le_write_commit,
    write_free: le_write_free,
    optimize: le_optimize,
    cleanup: le_cleanup,
    add_columns: le_add_columns,
    drop_columns: le_drop_columns,
    write_delete_read: le_write_delete_read,
});

/// The table of entry points, or NULL when the caller was built for
/// another ABI.
#[unsafe(no_mangle)]
pub extern "C" fn lance_engine_api(abi_version: u32) -> *const LeApi {
    if abi_version != ABI_VERSION {
        return std::ptr::null();
    }
    &*API as *const LeApi
}
