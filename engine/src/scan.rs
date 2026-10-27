// SPDX-License-Identifier: Apache-2.0
//! Scans and takes (docs/design.md §2.8): Lance's scanner over a version,
//! with the transaction's own fragments added and its deleted rows left out;
//! the batches decoded by a task of the runtime ahead of the caller, who
//! takes them one at a time.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float16Array, Float32Array, Float64Array, RecordBatch, UInt8Array,
    UInt64Array,
};
use arrow_schema::Field;
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::fragment::FileFragment;
use lance::dataset::rowids::get_row_id_index;
use lance::dataset::scanner::Scanner;
use lance_index::scalar::FullTextSearchQuery;
use lance_linalg::distance::DistanceType;
use lance_select::{RowAddrMask, RowAddrTreeMap};
use roaring::RoaringTreemap;
use tokio::sync::mpsc;

use crate::error::{EResult, EngineError, LE_ERR_INTERRUPTED};
use crate::ffi::*;
use crate::rt::engine;
use crate::types::{cast_for_reader, null_array};
use crate::write::{OwnView, WriteState};

pub struct NnSpec {
    pub column: String,
    pub query: ArrayRef,
    pub k: usize,
    pub metric: i32,
    pub nprobes: usize,
    pub ef: usize,
    pub refine: u32,
    pub use_index: bool,
}

pub struct ScanSpec {
    pub columns: Vec<String>,
    pub types: Option<Vec<Field>>,
    pub filter: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub with_row_id: bool,
    pub scan_in_order: bool,
    pub batch_size: Option<usize>,
    pub readahead: usize,
    pub fragments: Option<Vec<u64>>,
    pub nn: Option<NnSpec>,
    pub prefilter: bool,
    pub fts: Option<(Option<String>, String)>,
    pub write: Option<Arc<tokio::sync::Mutex<WriteState>>>,
    pub cid: i64,
}

impl ScanSpec {
    /// The C spec, copied out.
    ///
    /// # Safety
    /// `spec` points at a LeScanSpec whose pointers are valid for the call.
    pub unsafe fn from_c(spec: *const LeScanSpec) -> EResult<Self> {
        if spec.is_null() {
            return Err(EngineError::invalid("the scan spec is NULL"));
        }
        let s = unsafe { &*spec };
        if (s.size as usize) < std::mem::size_of::<LeScanSpec>() {
            return Err(EngineError::invalid("the scan spec is from an older ABI"));
        }
        let mut columns = Vec::with_capacity(s.ncolumns.max(0) as usize);
        for i in 0..s.ncolumns.max(0) as usize {
            let p = unsafe { *s.columns.add(i) };
            columns.push(unsafe { req_str(p, "a column name") }?.to_string());
        }
        let types = unsafe { crate::types::import_fields(s.types) }?;
        if let Some(t) = &types
            && t.len() != columns.len()
        {
            return Err(EngineError::invalid(format!(
                "{} types were given for {} columns",
                t.len(),
                columns.len()
            )));
        }
        let fragments = if s.nfragments >= 0 {
            let n = s.nfragments as usize;
            let slice = if n == 0 {
                &[][..]
            } else {
                unsafe { std::slice::from_raw_parts(s.fragments, n) }
            };
            Some(slice.to_vec())
        } else {
            None
        };
        let nn = match unsafe { opt_str(s.nn_column) }? {
            None => None,
            Some(column) => {
                let dim = s.nn_dim.max(0) as usize;
                if s.nn_query.is_null() || dim == 0 {
                    return Err(EngineError::invalid("a nearest-neighbour search needs a query vector"));
                }
                let query: ArrayRef = match s.nn_query_type {
                    LE_VEC_FLOAT32 => Arc::new(Float32Array::from(
                        unsafe { std::slice::from_raw_parts(s.nn_query as *const f32, dim) }.to_vec(),
                    )),
                    LE_VEC_FLOAT64 => Arc::new(Float64Array::from(
                        unsafe { std::slice::from_raw_parts(s.nn_query as *const f64, dim) }.to_vec(),
                    )),
                    LE_VEC_FLOAT16 => Arc::new(Float16Array::from(
                        unsafe { std::slice::from_raw_parts(s.nn_query as *const u16, dim) }
                            .iter()
                            .map(|b| half::f16::from_bits(*b))
                            .collect::<Vec<_>>(),
                    )),
                    LE_VEC_UINT8 => Arc::new(UInt8Array::from(
                        unsafe { std::slice::from_raw_parts(s.nn_query as *const u8, dim) }.to_vec(),
                    )),
                    t => return Err(EngineError::invalid(format!("unknown query vector type {t}"))),
                };
                if s.nn_k <= 0 {
                    return Err(EngineError::invalid("a nearest-neighbour search needs k > 0"));
                }
                Some(NnSpec {
                    column: column.to_string(),
                    query,
                    k: s.nn_k as usize,
                    metric: s.nn_metric,
                    nprobes: s.nn_nprobes.max(0) as usize,
                    ef: s.nn_ef.max(0) as usize,
                    refine: s.nn_refine.max(0) as u32,
                    use_index: s.nn_use_index,
                })
            }
        };
        let fts = unsafe { opt_str(s.fts_query) }?.map(|q| {
            (
                unsafe { opt_str(s.fts_column) }.ok().flatten().map(str::to_string),
                q.to_string(),
            )
        });
        let write = if s.write.is_null() {
            None
        } else {
            Some(unsafe { &*s.write }.inner.clone())
        };
        Ok(Self {
            columns,
            types,
            filter: unsafe { opt_str(s.filter) }?.map(str::to_string),
            limit: (s.limit >= 0).then_some(s.limit),
            offset: (s.offset > 0).then_some(s.offset),
            with_row_id: s.with_row_id,
            scan_in_order: s.scan_in_order,
            batch_size: (s.batch_size > 0).then_some(s.batch_size as usize),
            readahead: s.readahead.max(1) as usize,
            fragments,
            nn,
            prefilter: s.prefilter,
            fts,
            write,
            cid: s.cid,
        })
    }
}

fn metric(m: i32) -> EResult<Option<DistanceType>> {
    Ok(match m {
        LE_METRIC_DEFAULT => None,
        LE_METRIC_L2 => Some(DistanceType::L2),
        LE_METRIC_COSINE => Some(DistanceType::Cosine),
        LE_METRIC_DOT => Some(DistanceType::Dot),
        LE_METRIC_HAMMING => Some(DistanceType::Hamming),
        other => return Err(EngineError::invalid(format!("unknown distance type {other}"))),
    })
}

/// What the producer does to each batch Lance hands it.
struct Post {
    wanted: Vec<Field>,
    extras: Vec<&'static str>,
    hide: Option<RoaringTreemap>,
    keep_row_id: bool,
    /// limit and offset applied here, when hidden rows keep them from Lance
    limit: Option<i64>,
    offset: i64,
    seen: i64,
    emitted: i64,
}

impl Post {
    fn apply(&mut self, batch: RecordBatch) -> EResult<Option<RecordBatch>> {
        let mut batch = batch;
        if let Some(hide) = &self.hide {
            let ids = batch
                .column_by_name(lance_core::ROW_ID)
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>().cloned())
                .ok_or_else(|| EngineError::internal("the scan has no _rowid to filter by"))?;
            if ids.iter().any(|id| id.is_some_and(|v| hide.contains(v))) {
                let mask: BooleanArray = ids
                    .iter()
                    .map(|id| Some(!id.is_some_and(|v| hide.contains(v))))
                    .collect();
                batch = arrow_select::filter::filter_record_batch(&batch, &mask)?;
            }
        }
        if self.limit.is_some() || self.offset > 0 {
            let n = batch.num_rows() as i64;
            let skip = (self.offset - self.seen).clamp(0, n);
            self.seen += n;
            let mut take = n - skip;
            if let Some(limit) = self.limit {
                take = take.min((limit - self.emitted).max(0));
            }
            self.emitted += take;
            batch = batch.slice(skip as usize, take as usize);
        }
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        let mut extras = self.extras.clone();
        if !self.keep_row_id {
            extras.retain(|e| *e != lance_core::ROW_ID);
        }
        Ok(Some(cast_for_reader(&batch, &self.wanted, &extras)?))
    }

    fn finished(&self) -> bool {
        self.limit.is_some_and(|l| self.emitted >= l)
    }
}

fn wanted_fields(ds: &Dataset, spec: &ScanSpec) -> EResult<Vec<Field>> {
    match &spec.types {
        Some(t) => Ok(t.clone()),
        None => {
            let schema = arrow_schema::Schema::from(ds.schema());
            spec.columns
                .iter()
                .map(|c| {
                    schema
                        .field_with_name(c)
                        .map(|f| f.clone())
                        .map_err(|_| EngineError::not_found(format!("the dataset has no column \"{c}\"")))
                })
                .collect()
        }
    }
}

/// Lance's scanner for a spec, and what the producer does to its batches.
async fn build(ds: Arc<Dataset>, spec: &ScanSpec) -> EResult<(Scanner, Post)> {
    let view: Option<OwnView> = match &spec.write {
        Some(w) => {
            let mut w = w.lock().await;
            Some(w.view(spec.cid).await?)
        }
        None => None,
    };
    let wanted = wanted_fields(&ds, spec)?;
    let mut scanner = ds.scan();
    scanner.project(&spec.columns)?;

    let own_fragments = view.as_ref().map(|v| !v.fragments.is_empty()).unwrap_or(false);
    let truncated = view.as_ref().is_some_and(|v| v.truncated);
    if spec.fragments.is_some() || own_fragments || truncated {
        let mut fragments: Vec<lance_table::format::Fragment> = if truncated {
            Vec::new()
        } else {
            match &spec.fragments {
                Some(ids) => ds
                    .manifest()
                    .fragments
                    .iter()
                    .filter(|f| ids.contains(&f.id))
                    .cloned()
                    .collect(),
                None => ds.manifest().fragments.as_ref().clone(),
            }
        };
        if let Some(v) = &view
            && spec.fragments.is_none()
        {
            fragments.extend(v.fragments.iter().cloned());
        }
        scanner.with_fragments(fragments);
    }

    let hide = view
        .as_ref()
        .map(|v| v.hide.clone())
        .filter(|h| !h.is_empty());
    let need_row_id = spec.with_row_id || hide.is_some();
    if need_row_id {
        scanner.with_row_id();
    }
    if let Some(f) = &spec.filter {
        scanner.filter(f)?;
    }
    if let Some(bs) = spec.batch_size {
        scanner.batch_size(bs);
    }
    scanner.scan_in_order(spec.scan_in_order);

    let mut extras: Vec<&'static str> = vec![lance_core::ROW_ID];
    if let Some(nn) = &spec.nn {
        scanner.nearest(&nn.column, nn.query.as_ref(), nn.k)?;
        if let Some(m) = metric(nn.metric)? {
            scanner.distance_metric(m);
        }
        if nn.nprobes > 0 {
            scanner.nprobes(nn.nprobes);
        }
        if nn.ef > 0 {
            scanner.ef(nn.ef);
        }
        if nn.refine > 0 {
            scanner.refine(nn.refine);
        }
        scanner.use_index(nn.use_index);
        scanner.prefilter(spec.prefilter);
        extras.push("_distance");
    }
    if let Some((column, query)) = &spec.fts {
        let mut q = FullTextSearchQuery::new(query.clone());
        if let Some(c) = column {
            q = q.with_column(c.clone())?;
        }
        if let Some(l) = spec.limit {
            q = q.limit(Some(l + spec.offset.unwrap_or(0)));
        }
        scanner.full_text_search(q)?;
        scanner.prefilter(spec.prefilter);
        extras.push("_score");
    }
    // Rows deleted by the transaction stay out of a search's candidates.
    if (spec.nn.is_some() || spec.fts.is_some())
        && let Some(v) = &view
        && !v.deleted_committed.is_empty()
    {
        let block: RowAddrTreeMap = v.deleted_committed.iter().collect();
        scanner.with_row_id_prefilter(RowAddrMask::from_block(block));
    }

    let mut post = Post {
        wanted,
        extras,
        hide: hide.clone(),
        keep_row_id: spec.with_row_id,
        limit: None,
        offset: 0,
        seen: 0,
        emitted: 0,
    };
    if spec.nn.is_none() && (spec.limit.is_some() || spec.offset.is_some()) {
        if hide.is_some() {
            post.limit = spec.limit;
            post.offset = spec.offset.unwrap_or(0);
        } else {
            scanner.limit(spec.limit, spec.offset)?;
        }
    }
    Ok((scanner, post))
}

pub struct LeScan {
    rx: mpsc::Receiver<EResult<RecordBatch>>,
    task: tokio::task::JoinHandle<()>,
}

pub fn begin(ds: Arc<Dataset>, spec: ScanSpec) -> EResult<LeScan> {
    let engine = engine()?;
    let (tx, rx) = mpsc::channel(spec.readahead);
    let task = engine.rt.spawn(async move {
        let result: EResult<()> = async {
            let (scanner, mut post) = build(ds, &spec).await?;
            let mut stream = scanner.try_into_stream().await?;
            while let Some(batch) = stream.try_next().await? {
                if let Some(b) = post.apply(batch)?
                    && tx.send(Ok(b)).await.is_err()
                {
                    return Ok(());
                }
                if post.finished() {
                    return Ok(());
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            let _ = tx.send(Err(e)).await;
        }
    });
    Ok(LeScan { rx, task })
}

impl LeScan {
    /// The next batch, or None at the end.
    pub fn next(&mut self) -> EResult<Option<RecordBatch>> {
        let engine = engine()?;
        loop {
            let poll = engine.poll();
            let got = engine
                .rt
                .block_on(async { tokio::time::timeout(poll, self.rx.recv()).await });
            match got {
                Ok(Some(item)) => return item.map(Some),
                Ok(None) => return Ok(None),
                Err(_) => {
                    if engine.interrupted() {
                        return Err(EngineError::new(LE_ERR_INTERRUPTED, "canceled"));
                    }
                }
            }
        }
    }
}

impl Drop for LeScan {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub async fn explain(ds: Arc<Dataset>, spec: ScanSpec, verbose: bool) -> EResult<String> {
    let (scanner, _) = build(ds, &spec).await?;
    Ok(scanner.explain_plan(verbose).await?)
}

pub async fn count_rows(ds: Arc<Dataset>, spec: ScanSpec) -> EResult<u64> {
    let (scanner, post) = build(ds, &spec).await?;
    if post.hide.is_some() {
        // The transaction's own rows: counted as the rows a scan hands.
        let mut post = post;
        let mut n = 0u64;
        let mut stream = scanner.try_into_stream().await?;
        while let Some(batch) = stream.try_next().await? {
            if let Some(b) = post.apply(batch)? {
                n += b.num_rows() as u64;
            }
        }
        return Ok(n);
    }
    Ok(scanner.count_rows().await?)
}

/// Rows by row id: the transaction's own and the version's, in the order
/// asked, an id no visible row has left out.
pub async fn take(ds: Arc<Dataset>, spec: ScanSpec, row_ids: Vec<u64>) -> EResult<RecordBatch> {
    crate::trace!("take {} ids from version {}", row_ids.len(), ds.version_id());
    let wanted = wanted_fields(&ds, &spec)?;
    let projection = ds.schema().project(&spec.columns)?;
    let mut found: HashMap<u64, (usize, usize)> = HashMap::new(); // id -> (part, row)
    let mut parts: Vec<RecordBatch> = Vec::new();

    let mut committed: Vec<u64> = Vec::new();
    let mut truncated = false;
    if let Some(w) = &spec.write {
        crate::trace!("take: locking the write state");
        let mut w = w.lock().await;
        crate::trace!("take: view of cid {}", spec.cid);
        let view = w.view(spec.cid).await?;
        crate::trace!("take: viewed, {} own fragments", view.fragments.len());
        truncated = view.truncated;
        // The transaction's own rows, by fragment and offset.
        let mut by_fragment: HashMap<usize, Vec<(u32, u64)>> = HashMap::new();
        for &id in &row_ids {
            if view.hide.contains(id) {
                continue;
            }
            match w.locate(id) {
                Some((fi, pos)) => by_fragment.entry(fi).or_default().push((pos as u32, id)),
                None => committed.push(id),
            }
        }
        for (fi, mut rows) in by_fragment {
            rows.sort_unstable();
            let offsets: Vec<u32> = rows.iter().map(|r| r.0).collect();
            let fragment = w.fragments[fi].scan_fragment();
            let ff = FileFragment::new(w.base(), fragment);
            let batch = ff.take(&offsets, &projection).await?;
            let part = parts.len();
            for (i, (_, id)) in rows.iter().enumerate() {
                found.insert(*id, (part, i));
            }
            parts.push(batch);
        }
    } else {
        committed = row_ids.clone();
    }

    if !truncated && !committed.is_empty() {
        crate::trace!("take: row id index for {} committed ids", committed.len());
        let live: Vec<u64> = match get_row_id_index(&ds).await? {
            Some(index) => {
                let mut v = Vec::with_capacity(committed.len());
                for id in committed {
                    if index.get(id)?.is_some() {
                        v.push(id);
                    }
                }
                v
            }
            // Without stable row ids, ids are addresses.
            None => committed,
        };
        crate::trace!("take: {} live ids", live.len());
        if !live.is_empty() {
            let batch = ds.take_rows(&live, projection.clone()).await?;
            crate::trace!("take: took {} rows", batch.num_rows());
            let part = parts.len();
            for (i, id) in live.iter().enumerate() {
                found.insert(*id, (part, i));
            }
            parts.push(batch);
        }
    }

    // Assemble in the order asked.
    let mut order: Vec<(usize, usize, u64)> = Vec::new();
    for id in &row_ids {
        if let Some((p, r)) = found.remove(id) {
            order.push((p, r, *id));
        }
    }
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(wanted.len() + 1);
    let mut fields: Vec<Field> = Vec::with_capacity(wanted.len() + 1);
    for f in &wanted {
        let pieces: Vec<ArrayRef> = parts
            .iter()
            .map(|b| {
                b.column_by_name(f.name())
                    .cloned()
                    .unwrap_or_else(|| null_array(f, b.num_rows()))
            })
            .map(|c| crate::types::cast_column(&c, f.data_type(), f.name()))
            .collect::<EResult<_>>()?;
        let refs: Vec<&dyn Array> = pieces.iter().map(|a| a.as_ref()).collect();
        let indices: Vec<(usize, usize)> = order.iter().map(|(p, r, _)| (*p, *r)).collect();
        let column = if refs.is_empty() {
            null_array(f, 0)
        } else {
            arrow_select::interleave::interleave(&refs, &indices)?
        };
        fields.push(Field::new(f.name(), f.data_type().clone(), true));
        columns.push(column);
    }
    fields.push(Field::new(lance_core::ROW_ID, arrow_schema::DataType::UInt64, true));
    columns.push(Arc::new(UInt64Array::from(
        order.iter().map(|o| o.2).collect::<Vec<_>>(),
    )));
    Ok(RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)?)
}

/// Rows by their positions among the version's live rows, in the order asked.
pub async fn take_positions(ds: Arc<Dataset>, spec: ScanSpec, positions: Vec<u64>) -> EResult<RecordBatch> {
    let wanted = wanted_fields(&ds, &spec)?;
    let projection = ds.schema().project(&spec.columns)?;
    let total = ds.count_rows(None).await? as u64;
    let positions: Vec<u64> = positions.into_iter().filter(|p| *p < total).collect();
    let batch = if positions.is_empty() {
        RecordBatch::new_empty(Arc::new(arrow_schema::Schema::from(&projection)))
    } else {
        ds.take(&positions, projection).await?
    };
    cast_for_reader(&batch, &wanted, &[])
}
