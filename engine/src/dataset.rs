// SPDX-License-Identifier: Apache-2.0
//! Datasets: opening one at a version, what its manifest says, its history.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance::index::DatasetIndexExt;
use lance::io::{ObjectStore, ObjectStoreParams, StorageOptionsAccessor};
use lance_table::rowids::segment::U64Segment;

use crate::error::{EResult, EngineError};
use crate::rt::session;

/// A dataset at one version, as the C ABI hands it out.
pub struct LeDataset {
    pub ds: Arc<Dataset>,
}

impl LeDataset {
    pub fn new(ds: Arc<Dataset>) -> Self {
        Self { ds }
    }
}

pub fn store_params(options: &HashMap<String, String>) -> ObjectStoreParams {
    ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            options.clone(),
        ))),
        ..Default::default()
    }
}

fn options_hash(options: &HashMap<String, String>) -> u64 {
    let mut pairs: Vec<_> = options.iter().collect();
    pairs.sort();
    let mut h = DefaultHasher::new();
    pairs.hash(&mut h);
    h.finish()
}

/// Datasets opened at a fixed version, kept for the next scan of the same
/// version: opening one is a manifest decode even when the session caches
/// the bytes.
struct OpenCache {
    entries: HashMap<(String, u64, u64), Arc<Dataset>>,
    order: VecDeque<(String, u64, u64)>,
}

const OPEN_CACHE_SIZE: usize = 128;

static OPEN_CACHE: Mutex<Option<OpenCache>> = Mutex::new(None);

fn cache_get(key: &(String, u64, u64)) -> Option<Arc<Dataset>> {
    let guard = OPEN_CACHE.lock().ok()?;
    guard.as_ref()?.entries.get(key).cloned()
}

fn cache_put(key: (String, u64, u64), ds: Arc<Dataset>) {
    let Ok(mut guard) = OPEN_CACHE.lock() else {
        return;
    };
    let cache = guard.get_or_insert_with(|| OpenCache {
        entries: HashMap::new(),
        order: VecDeque::new(),
    });
    if cache.entries.insert(key.clone(), ds).is_none() {
        cache.order.push_back(key);
        while cache.order.len() > OPEN_CACHE_SIZE {
            if let Some(old) = cache.order.pop_front() {
                cache.entries.remove(&old);
            }
        }
    }
}

/// Forget every cached version of a dataset: it was removed or replaced.
pub fn cache_forget(uri: &str) {
    if let Ok(mut guard) = OPEN_CACHE.lock()
        && let Some(cache) = guard.as_mut()
    {
        cache.entries.retain(|k, _| k.0 != uri);
        cache.order.retain(|k| k.0 != uri);
    }
}

/// Open a dataset at a version, or at its latest when `version` is None.
pub async fn open(
    uri: &str,
    options: &HashMap<String, String>,
    version: Option<u64>,
) -> EResult<Arc<Dataset>> {
    let key = version.map(|v| (uri.to_string(), v, options_hash(options)));
    if let Some(key) = &key
        && let Some(ds) = cache_get(key)
    {
        return Ok(ds);
    }
    let mut builder = DatasetBuilder::from_uri(uri)
        .with_session(session()?)
        .with_storage_options(options.clone());
    if let Some(v) = version {
        builder = builder.with_version(v);
    }
    let ds = Arc::new(builder.load().await?);
    let key = key.unwrap_or_else(|| (uri.to_string(), ds.version_id(), options_hash(options)));
    cache_put(key, ds.clone());
    Ok(ds)
}

pub struct Info {
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

/// The row ids of the version, deleted rows included: the highest a
/// fragment holds, or the one before Lance's own next row id, above which
/// the server's allocator starts its new chunks; and whether one of them,
/// or one Lance gave, is in `lo..hi`, the ids a backend's chunk has left.
pub async fn row_ids_held(ds: &Dataset, lo: u64, hi: u64) -> EResult<(u64, bool)> {
    let next = ds.manifest().next_row_id;
    let mut max = next.saturating_sub(1);
    let mut held = lo < hi && lo < next;
    if !ds.manifest().uses_stable_row_ids() {
        return Ok((max, held));
    }
    for f in ds.manifest().fragments.iter() {
        if f.row_id_meta.is_none() {
            continue;
        }
        let seq = lance::dataset::rowids::load_row_id_sequence(ds, f).await?;
        for segment in seq.segments() {
            if let Some(r) = segment.range() {
                max = max.max(*r.end());
            }
            held = held || segment_holds(segment, lo, hi);
        }
    }
    Ok((max, held))
}

/// Whether the segment holds an id in `lo..hi`: the first of its sorted
/// ids at or above `lo` found by its position.
fn segment_holds(segment: &U64Segment, lo: u64, hi: u64) -> bool {
    match segment.range() {
        Some(r) if *r.start() < hi && *r.end() >= lo => {}
        _ => return false,
    }
    match segment {
        U64Segment::Range(_) => true,
        U64Segment::Array(_) => segment.iter().any(|id| (lo..hi).contains(&id)),
        _ => {
            let (mut a, mut b) = (0, segment.len());
            while a < b {
                let m = a + (b - a) / 2;
                match segment.get(m) {
                    Some(id) if id < lo => a = m + 1,
                    _ => b = m,
                }
            }
            segment.get(a).is_some_and(|id| id < hi)
        }
    }
}

/// What the manifest says of a version, without reading a data file: a
/// fragment whose deletion count the manifest lacks counts as whole.
pub fn info(ds: &Dataset) -> Info {
    let manifest = ds.manifest();
    let mut num_rows = 0u64;
    let mut deleted = 0u64;
    let mut bytes = 0u64;
    for f in manifest.fragments.iter() {
        let physical = f.physical_rows.unwrap_or(0) as u64;
        let dead = f
            .deletion_file
            .as_ref()
            .and_then(|d| d.num_deleted_rows)
            .unwrap_or(0) as u64;
        num_rows += physical.saturating_sub(dead);
        deleted += dead;
        for file in &f.files {
            if let Some(size) = file.file_size_bytes.get() {
                bytes += size.get();
            }
        }
    }
    Info {
        version: manifest.version,
        num_rows,
        num_deleted_rows: deleted,
        num_fragments: manifest.fragments.len() as u64,
        data_bytes: bytes,
        max_fragment_id: manifest.max_fragment_id().unwrap_or(0),
        max_row_id: 0,
        stable_row_ids: manifest.uses_stable_row_ids(),
        timestamp_us: (manifest.timestamp_nanos / 1000) as i64,
    }
}

pub fn arrow_schema(ds: &Dataset) -> arrow_schema::Schema {
    arrow_schema::Schema::from(ds.schema())
}

/// Versions newer than `after`, each with the properties its transaction
/// was committed with, as a JSON array; `after` 0: every version, without
/// them.
pub async fn versions_json(ds: Arc<Dataset>, after: u64) -> EResult<String> {
    let mut out = Vec::new();
    if after == 0 {
        for v in ds.versions().await? {
            out.push(serde_json::json!({
                "version": v.version,
                "timestamp": v.timestamp.to_rfc3339(),
                "metadata": v.metadata,
            }));
        }
    } else {
        let latest = ds.latest_version_id().await?;
        let mut v = after + 1;
        while v <= latest {
            let properties = match ds.read_transaction_by_version(v).await {
                Ok(Some(tx)) => {
                    let props: HashMap<String, String> = tx
                        .transaction_properties
                        .as_ref()
                        .map(|p| p.as_ref().clone())
                        .unwrap_or_default();
                    serde_json::json!({
                        "operation": tx.operation.to_string(),
                        "properties": props,
                    })
                }
                Ok(None) => serde_json::json!({ "operation": null, "properties": {} }),
                // A cleaned-up version, or one whose transaction file is
                // gone: reported as unknown.
                Err(_) => serde_json::json!({ "operation": null, "properties": {}, "missing": true }),
            };
            let mut entry = serde_json::json!({ "version": v });
            if let (Some(obj), Some(p)) = (entry.as_object_mut(), properties.as_object()) {
                for (k, val) in p {
                    obj.insert(k.clone(), val.clone());
                }
            }
            out.push(entry);
            v += 1;
        }
    }
    Ok(serde_json::to_string(&out)?)
}

/// The version's indices, as a JSON array.
pub async fn indices_json(ds: Arc<Dataset>) -> EResult<String> {
    let indices = ds.load_indices().await?;
    let schema = ds.schema();
    let mut out = Vec::new();
    for idx in indices.iter() {
        let columns: Vec<String> = idx
            .fields
            .iter()
            .filter_map(|id| schema.field_by_id(*id).map(|f| f.name.clone()))
            .collect();
        let fragments = idx
            .fragment_bitmap
            .as_ref()
            .map(|b| b.len())
            .unwrap_or(0);
        // The live rows of the version's fragments the index covers: the
        // rest a filter it answers scans without it.
        let rows: u64 = idx
            .fragment_bitmap
            .as_ref()
            .map(|b| {
                ds.manifest()
                    .fragments
                    .iter()
                    .filter(|f| b.contains(f.id as u32))
                    .map(|f| f.num_rows().or(f.physical_rows).unwrap_or(0) as u64)
                    .sum()
            })
            .unwrap_or(0);
        let index_type = idx
            .index_details
            .as_ref()
            .map(|d| d.type_url.rsplit('.').next().unwrap_or("").to_string())
            .unwrap_or_default();
        out.push(serde_json::json!({
            "name": idx.name,
            "uuid": idx.uuid.to_string(),
            "columns": columns,
            "type": index_type,
            "dataset_version": idx.dataset_version,
            "fragments": fragments,
            "rows": rows,
        }));
    }
    Ok(serde_json::to_string(&out)?)
}

/// The version's fragments, as a JSON array.
pub fn fragments_json(ds: &Dataset) -> EResult<String> {
    let mut out = Vec::new();
    for f in ds.manifest().fragments.iter() {
        let files: Vec<String> = f.files.iter().map(|d| d.path.clone()).collect();
        out.push(serde_json::json!({
            "id": f.id,
            "physical_rows": f.physical_rows,
            "deleted_rows": f.deletion_file.as_ref().and_then(|d| d.num_deleted_rows),
            "files": files,
        }));
    }
    Ok(serde_json::to_string(&out)?)
}

pub fn fragment_list(ds: &Dataset) -> (Vec<u64>, Vec<u64>) {
    let mut ids = Vec::new();
    let mut rows = Vec::new();
    for f in ds.manifest().fragments.iter() {
        ids.push(f.id);
        rows.push(f.num_rows().or(f.physical_rows).unwrap_or(0) as u64);
    }
    (ids, rows)
}

/// Make `version` the latest again, as a new version.
pub async fn restore(
    uri: &str,
    options: &HashMap<String, String>,
    version: u64,
    properties: HashMap<String, String>,
) -> EResult<u64> {
    use lance::dataset::CommitBuilder;
    use lance::dataset::transaction::{Operation, TransactionBuilder};

    let latest = open(uri, options, None).await?;
    let tx = TransactionBuilder::new(latest.version_id(), Operation::Restore { version })
        .transaction_properties(Some(Arc::new(properties)))
        .build();
    let ds = CommitBuilder::new(latest)
        .with_session(session()?)
        .execute(tx)
        .await?;
    Ok(ds.version_id())
}

/// Remove a dataset's directory and everything in it.
pub async fn remove(uri: &str, options: &HashMap<String, String>) -> EResult<()> {
    let registry = session()?.store_registry();
    let params = store_params(options);
    let (store, path) = ObjectStore::from_uri_and_params(registry, uri, &params)
        .await
        .map_err(EngineError::from)?;
    store.remove_dir_all(path).await.map_err(EngineError::from)?;
    cache_forget(uri);
    Ok(())
}

/// An empty dataset of the schema at the location, its rows carrying stable
/// row ids.
pub async fn create(
    uri: &str,
    options: &HashMap<String, String>,
    schema: arrow_schema::Schema,
    storage_version: Option<lance_file::version::LanceFileVersion>,
    properties: HashMap<String, String>,
) -> EResult<u64> {
    use lance::dataset::{WriteMode, WriteParams};

    let schema = Arc::new(schema);
    let reader = arrow_array::RecordBatchIterator::new(
        Vec::<std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>>::new(),
        schema,
    );
    let params = WriteParams {
        mode: WriteMode::Create,
        enable_stable_row_ids: true,
        data_storage_version: storage_version,
        store_params: Some(store_params(options)),
        session: Some(session()?),
        skip_auto_cleanup: true,
        transaction_properties: Some(Arc::new(properties)),
        ..Default::default()
    };
    let ds = Dataset::write(reader, uri, Some(params)).await?;
    cache_forget(uri);
    Ok(ds.version_id())
}
