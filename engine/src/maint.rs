// SPDX-License-Identifier: Apache-2.0
//! Index builds, compaction and cleanup (docs/design.md §2.11, §2.12).

use std::sync::Arc;

use lance::Dataset;
use lance::dataset::optimize::{CompactionOptions, compact_files};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance_index::IndexParams;
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::ScalarIndexParams;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_linalg::distance::DistanceType;
use lance_table::format::IndexMetadata;
use serde_json::{Map, Value};

use crate::error::{EResult, EngineError};

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

/// Compaction, then the indices brought up to the new fragments; each a
/// version of its own.  Returns the last version and what was done.
pub async fn optimize(base: Arc<Dataset>, options: Option<&str>) -> EResult<(u64, String)> {
    let p: Map<String, Value> = match options {
        Some(j) if !j.trim().is_empty() => match serde_json::from_str::<Value>(j)? {
            Value::Object(m) => m,
            _ => return Err(EngineError::invalid("optimize options must be a JSON object")),
        },
        _ => Map::new(),
    };
    let mut ds = base.as_ref().clone();
    let mut compaction = CompactionOptions::default();
    if let Some(n) = get_usize(&p, "target_rows_per_fragment")? {
        compaction.target_rows_per_fragment = n;
    }
    if let Some(Value::Bool(b)) = p.get("materialize_deletions") {
        compaction.materialize_deletions = *b;
    }
    if let Some(n) = p.get("materialize_deletions_threshold").and_then(|v| v.as_f64()) {
        compaction.materialize_deletions_threshold = n as f32;
    }
    let compact = !matches!(p.get("compact"), Some(Value::Bool(false)));
    let indices = !matches!(p.get("indices"), Some(Value::Bool(false)));
    let mut result = serde_json::json!({});
    if compact {
        let metrics = compact_files(&mut ds, compaction, None).await?;
        result["fragments_removed"] = metrics.fragments_removed.into();
        result["fragments_added"] = metrics.fragments_added.into();
        result["files_removed"] = metrics.files_removed.into();
        result["files_added"] = metrics.files_added.into();
    }
    if indices && !ds.load_indices().await?.is_empty() {
        ds.optimize_indices(&OptimizeOptions::default()).await?;
        result["indices_optimized"] = true.into();
    }
    result["version"] = ds.version_id().into();
    Ok((ds.version_id(), serde_json::to_string(&result)?))
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
