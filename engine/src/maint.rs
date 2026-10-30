// SPDX-License-Identifier: Apache-2.0
//! Index builds, compaction and cleanup (docs/design.md §2.11, §2.12).

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::index::DatasetIndexRemapperOptions;
use lance::dataset::optimize::{
    CompactionMetrics, CompactionOptions, IndexRemapMode, RewriteResult, commit_compaction,
    plan_compaction,
};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_index::IndexParams;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::ScalarIndexParams;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_linalg::distance::DistanceType;
use lance_table::format::{Fragment, IndexMetadata, RowIdMeta};
use lance_table::rowids::read_row_ids;
use serde_json::{Map, Value};

use crate::error::{EResult, EngineError, LE_ERR_CONFLICT};
use crate::write::row_id_meta;

fn get_usize(p: &Map<String, Value>, key: &str) -> EResult<Option<usize>> {
    match p.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(|v| Some(v as usize))
            .ok_or_else(|| EngineError::invalid(format!("index option \"{key}\" must be a positive integer"))),
        Some(Value::String(s)) => s
            .trim()
            .parse::<usize>()
            .map(Some)
            .map_err(|_| EngineError::invalid(format!("index option \"{key}\" must be a positive integer, not \"{s}\""))),
        Some(other) => Err(EngineError::invalid(format!(
            "index option \"{key}\" must be a positive integer, not {other}"
        ))),
    }
}

fn get_str<'a>(p: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    p.get(key).and_then(|v| v.as_str())
}

pub fn distance_type(name: &str) -> EResult<DistanceType> {
    match name.to_ascii_lowercase().as_str() {
        "l2" | "euclidean" => Ok(DistanceType::L2),
        "cosine" => Ok(DistanceType::Cosine),
        "dot" | "ip" | "inner_product" => Ok(DistanceType::Dot),
        "hamming" => Ok(DistanceType::Hamming),
        other => Err(EngineError::invalid(format!(
            "unknown distance type \"{other}\": l2, cosine, dot or hamming"
        ))),
    }
}

/// The vector column's dimension, for the defaults that depend on it.
fn vector_dim(ds: &Dataset, column: &str) -> Option<usize> {
    let field = ds.schema().field(column)?;
    match field.data_type() {
        arrow_schema::DataType::FixedSizeList(_, n) => Some(n as usize),
        arrow_schema::DataType::List(child) | arrow_schema::DataType::LargeList(child) => {
            match child.data_type() {
                arrow_schema::DataType::FixedSizeList(_, n) => Some(*n as usize),
                _ => None,
            }
        }
        _ => None,
    }
}

fn vector_params(ds: &Dataset, column: &str, kind: &str, p: &Map<String, Value>) -> EResult<VectorIndexParams> {
    let metric = distance_type(get_str(p, "metric").or_else(|| get_str(p, "distance_type")).unwrap_or("l2"))?;
    let mut ivf = IvfBuildParams::default();
    if let Some(n) = get_usize(p, "num_partitions")? {
        ivf.num_partitions = Some(n);
    }
    if let Some(n) = get_usize(p, "target_partition_size")? {
        ivf.target_partition_size = Some(n);
    }
    if let Some(n) = get_usize(p, "max_iterations")? {
        ivf.max_iters = n;
    }
    if let Some(n) = get_usize(p, "sample_rate")? {
        ivf.sample_rate = n;
    }
    let pq = || -> EResult<PQBuildParams> {
        let mut pq = PQBuildParams::default();
        match get_usize(p, "num_sub_vectors")? {
            Some(n) => pq.num_sub_vectors = n,
            None => {
                if let Some(dim) = vector_dim(ds, column) {
                    pq.num_sub_vectors = if dim % 16 == 0 {
                        (dim / 16).max(1)
                    } else if dim % 8 == 0 {
                        (dim / 8).max(1)
                    } else {
                        1
                    };
                }
            }
        }
        if let Some(n) = get_usize(p, "num_bits")? {
            pq.num_bits = n;
        }
        if let Some(n) = get_usize(p, "max_iterations")? {
            pq.max_iters = n;
        }
        Ok(pq)
    };
    let sq = || -> EResult<SQBuildParams> {
        let mut sq = SQBuildParams::default();
        if let Some(n) = get_usize(p, "num_bits")? {
            sq.num_bits = n as u16;
        }
        Ok(sq)
    };
    let hnsw = || -> EResult<HnswBuildParams> {
        let mut h = HnswBuildParams::default();
        if let Some(n) = get_usize(p, "m")? {
            h.m = n;
        }
        if let Some(n) = get_usize(p, "ef_construction")? {
            h.ef_construction = n;
        }
        Ok(h)
    };
    Ok(match kind {
        "IVF_FLAT" => VectorIndexParams::with_ivf_flat_params(metric, ivf),
        "IVF_PQ" => VectorIndexParams::with_ivf_pq_params(metric, ivf, pq()?),
        "IVF_SQ" => VectorIndexParams::with_ivf_sq_params(metric, ivf, sq()?),
        "IVF_HNSW_FLAT" => VectorIndexParams::ivf_hnsw(metric, ivf, hnsw()?),
        "IVF_HNSW_PQ" => VectorIndexParams::with_ivf_hnsw_pq_params(metric, ivf, hnsw()?, pq()?),
        "IVF_HNSW_SQ" => VectorIndexParams::with_ivf_hnsw_sq_params(metric, ivf, hnsw()?, sq()?),
        "IVF_RQ" => {
            let bits = get_usize(p, "num_bits")?.unwrap_or(1) as u8;
            let mut v = VectorIndexParams::ivf_rq(1, bits, metric);
            if let Some(lance::index::vector::StageParams::Ivf(i)) = v.stages.get_mut(0) {
                *i = ivf;
            }
            v
        }
        other => return Err(EngineError::invalid(format!("unknown vector index type \"{other}\""))),
    })
}

/// The index's files written for `base`, not committed.
pub async fn build_index(
    base: Arc<Dataset>,
    column: &str,
    index_type: &str,
    params_json: Option<&str>,
    name: Option<&str>,
    replace: bool,
) -> EResult<IndexMetadata> {
    let params: Map<String, Value> = match params_json {
        Some(j) if !j.trim().is_empty() => match serde_json::from_str::<Value>(j)? {
            Value::Object(m) => m,
            _ => return Err(EngineError::invalid("index options must be a JSON object")),
        },
        _ => Map::new(),
    };
    let kind = index_type.to_ascii_uppercase().replace('-', "_");
    let mut ds = base.as_ref().clone();
    let boxed: Box<dyn IndexParams>;
    let itype;
    if kind.starts_with("IVF_") {
        boxed = Box::new(vector_params(&ds, column, &kind, &params)?);
        itype = IndexType::Vector;
    } else {
        let scalar = match kind.as_str() {
            "BTREE" => "btree",
            "BITMAP" => "bitmap",
            "LABEL_LIST" | "LABELLIST" => "labellist",
            "INVERTED" | "FTS" => "inverted",
            "NGRAM" => "ngram",
            "ZONEMAP" | "ZONE_MAP" => "zonemap",
            "BLOOMFILTER" | "BLOOM_FILTER" => "bloomfilter",
            other => {
                return Err(EngineError::invalid(format!(
                    "unknown index type \"{other}\": IVF_PQ, IVF_FLAT, IVF_SQ, IVF_HNSW_SQ, IVF_HNSW_PQ, IVF_HNSW_FLAT, IVF_RQ, BTREE, BITMAP, LABEL_LIST, INVERTED, NGRAM, ZONEMAP or BLOOMFILTER"
                )));
            }
        };
        let mut rest = params.clone();
        rest.remove("type");
        let mut sp = ScalarIndexParams::new(scalar.to_string());
        if !rest.is_empty() {
            sp.params = Some(serde_json::to_string(&Value::Object(rest))?);
        }
        boxed = Box::new(sp);
        itype = IndexType::Scalar;
    }
    let cols = [column];
    let mut builder = ds.create_index_builder(&cols, itype, boxed.as_ref()).replace(replace);
    if let Some(n) = name {
        builder = builder.name(n.to_string());
    }
    Ok(builder.execute_uncommitted().await?)
}

fn options_object(options: Option<&str>) -> EResult<Map<String, Value>> {
    match options {
        Some(j) if !j.trim().is_empty() => match serde_json::from_str::<Value>(j)? {
            Value::Object(m) => Ok(m),
            _ => Err(EngineError::invalid("optimize options must be a JSON object")),
        },
        _ => Ok(Map::new()),
    }
}

fn compaction_options(p: &Map<String, Value>) -> EResult<CompactionOptions> {
    let mut compaction = CompactionOptions::default();
    // The commit moves the rows of the indices that hold row addresses, under
    // the table's commit lock.  Lance's default keeps a hash map's entry for
    // every row rewritten, which nothing here bounds; this reckons each move
    // from the rewrite's ranges.
    compaction.index_remap_mode = IndexRemapMode::Compact;
    if let Some(n) = get_usize(p, "target_rows_per_fragment")? {
        compaction.target_rows_per_fragment = n;
    }
    if let Some(Value::Bool(b)) = p.get("materialize_deletions") {
        compaction.materialize_deletions = *b;
    }
    if let Some(n) = p.get("materialize_deletions_threshold").and_then(|v| v.as_f64()) {
        compaction.materialize_deletions_threshold = n as f32;
    }
    // what a run may use: its rewrite tasks at once, each one's batches and
    // the bytes its scan reads ahead (0 would be Lance's unbounded), and
    // how much of the dataset it takes on
    if let Some(n) = get_usize(p, "num_threads")? {
        compaction.num_threads = Some(n.max(1));
    }
    if let Some(n) = get_usize(p, "batch_size")? {
        compaction.batch_size = Some(n.max(1));
    }
    if let Some(n) = get_usize(p, "io_buffer_size")? {
        compaction.io_buffer_size = Some(n.max(1) as u64);
    }
    if let Some(n) = get_usize(p, "max_source_fragments")? {
        compaction.max_source_fragments = Some(n);
    }
    if let Some(n) = get_usize(p, "max_source_rows")? {
        compaction.max_source_rows = Some(n);
    }
    if let Some(n) = get_usize(p, "max_source_bytes")? {
        compaction.max_source_bytes = Some(n as u64);
    }
    Ok(compaction)
}

fn note_metrics(result: &mut Value, metrics: &CompactionMetrics) {
    result["fragments_removed"] = metrics.fragments_removed.into();
    result["fragments_added"] = metrics.fragments_added.into();
    result["files_removed"] = metrics.files_removed.into();
    result["files_added"] = metrics.files_added.into();
}

/// Compaction, then the indices brought up to the new fragments; each a
/// version of its own.  Returns the last version and what was done.
pub async fn optimize(base: Arc<Dataset>, options: Option<&str>) -> EResult<(u64, String)> {
    let p = options_object(options)?;
    let mut ds = base.as_ref().clone();
    let indices = !matches!(p.get("indices"), Some(Value::Bool(false)));
    let mut result = serde_json::json!({});
    if let Some(mut c) = compact_prepare(base.clone(), options).await? {
        let (committed, metrics) = commit(&mut c, base).await?;
        note_metrics(&mut result, &metrics);
        ds = committed;
    }
    if indices && !ds.load_indices().await?.is_empty() {
        ds.optimize_indices(&OptimizeOptions::default()).await?;
        result["indices_optimized"] = true.into();
    }
    result["version"] = ds.version_id().into();
    Ok((ds.version_id(), serde_json::to_string(&result)?))
}

/// Compaction's new fragments, written on a version and not committed: the
/// rewrite, which takes long, apart from the commit, which a writer of the
/// table waits for.
pub struct Compaction {
    base: Arc<Dataset>,
    options: CompactionOptions,
    results: Vec<RewriteResult>,
}

/// Compaction as optimize() makes it, planned on `base` and its tasks run,
/// `num_threads` at once, without its commit; None when it has nothing to
/// do.
pub async fn compact_prepare(base: Arc<Dataset>, options: Option<&str>) -> EResult<Option<Compaction>> {
    let p = options_object(options)?;
    if matches!(p.get("compact"), Some(Value::Bool(false))) {
        return Ok(None);
    }
    let options = compaction_options(&p)?;
    let plan = plan_compaction(&base, &options).await?;
    if plan.num_tasks() == 0 {
        return Ok(None);
    }
    let threads = options.num_threads.unwrap_or_else(get_num_compute_intensive_cpus);
    let ds = base.as_ref();
    let mut results: Vec<RewriteResult> = futures::stream::iter(plan.compaction_tasks())
        .map(|task| async move { task.execute(ds).await })
        .buffer_unordered(threads)
        .try_collect()
        .await?;
    for f in results.iter_mut().flat_map(|r| r.new_fragments.iter_mut()) {
        reencode_row_ids(f)?;
    }
    Ok(Some(Compaction {
        base,
        options,
        results,
    }))
}

/// A new fragment's row ids in the encoding the server writes them in
/// (`row_id_meta()` in write.rs, which says why), where smaller: Lance
/// makes the part of a fragment it cuts at a new fragment's bound one
/// segment, a bitmap of its span where writers interleaved their chunks of
/// ids, which the manifest and the commit's transaction would both keep.
fn reencode_row_ids(f: &mut Fragment) -> EResult<()> {
    let Some(RowIdMeta::Inline(data)) = &f.row_id_meta else {
        return Ok(());
    };
    let ids: Vec<u64> = read_row_ids(data)?.iter().collect();
    let meta = row_id_meta(&ids);
    if let RowIdMeta::Inline(new) = &meta
        && new.len() < data.len()
    {
        f.row_id_meta = Some(meta);
    }
    Ok(())
}

/// The compaction committed on `latest`, the newest version, at or after
/// the one it was written on: Lance refuses it, as a conflict, when a
/// commit since then deleted rows of a fragment it replaces, and its files
/// are removed -- not after another failure, which may have come once its
/// version was published.  Returns the version and what was done.
pub async fn compact_commit(c: &mut Compaction, latest: Arc<Dataset>) -> EResult<(u64, String)> {
    let (ds, metrics) = commit(c, latest).await?;
    let mut result = serde_json::json!({});
    note_metrics(&mut result, &metrics);
    result["version"] = ds.version_id().into();
    Ok((ds.version_id(), serde_json::to_string(&result)?))
}

async fn commit(c: &mut Compaction, latest: Arc<Dataset>) -> EResult<(Dataset, CompactionMetrics)> {
    let results = std::mem::take(&mut c.results);
    if results.is_empty() {
        return Err(EngineError::invalid("the compaction has no files to commit"));
    }
    let added = written(&results);
    let mut ds = latest.as_ref().clone();
    match commit_compaction(
        &mut ds,
        results,
        Arc::new(DatasetIndexRemapperOptions::default()),
        &c.options,
    )
    .await
    {
        Ok(metrics) => Ok((ds, metrics)),
        Err(e) => {
            let e = EngineError::from(e);
            if e.code == LE_ERR_CONFLICT {
                remove(&c.base, added).await;
            }
            Err(e)
        }
    }
}

/// The files the compaction wrote and did not commit, removed.
pub async fn compact_cleanup(c: &mut Compaction) {
    let results = std::mem::take(&mut c.results);
    remove(&c.base, written(&results)).await;
}

fn written(results: &[RewriteResult]) -> Vec<String> {
    results
        .iter()
        .flat_map(|r| r.new_fragments.iter())
        .flat_map(|f| f.files.iter())
        .map(|f| f.path.clone())
        .collect()
}

async fn remove(base: &Dataset, files: Vec<String>) {
    if files.is_empty() {
        return;
    }
    let Ok(store) = base.object_store(None).await else {
        return;
    };
    let dir = base.data_dir();
    for f in files {
        let _ = store.delete(&object_store::path::Path::from(format!("{dir}/{f}"))).await;
    }
}

pub async fn cleanup(base: Arc<Dataset>, older_than_seconds: i64) -> EResult<String> {
    let stats = base
        .cleanup_old_versions(
            chrono::Duration::seconds(older_than_seconds.max(0)),
            Some(false),
            Some(false),
        )
        .await?;
    Ok(serde_json::to_string(&serde_json::json!({
        "bytes_removed": stats.bytes_removed,
        "old_versions": stats.old_versions,
        "data_files_removed": stats.data_files_removed,
        "index_files_removed": stats.index_files_removed,
        "deletion_files_removed": stats.deletion_files_removed,
    }))?)
}
