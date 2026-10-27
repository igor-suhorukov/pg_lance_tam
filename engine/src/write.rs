// SPDX-License-Identifier: Apache-2.0
//! One transaction's changes to one table (docs/design.md §2.6, §2.7).
//!
//! Rows arrive with the row ids the server gave them, so that a row's TID
//! is final when it is inserted; they wait in memory, then in fragments
//! written but not committed.  Each change is tagged with the command and
//! the subtransaction that made it: the first decides what the
//! transaction's own scans see, the second what a ROLLBACK TO SAVEPOINT
//! takes back.  At commit the changes become one Lance transaction on the
//! latest version the server's catalog has committed: appends, deletion
//! vectors for the rows the transaction deleted, and the pending index
//! builds, each committed version tagged with the server's properties.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::fragment::FileFragment;
use lance::dataset::rowids::get_row_id_index;
use lance::dataset::transaction::{Operation, TransactionBuilder, UpdateMode};
use lance::dataset::{CommitBuilder, InsertBuilder, WriteDestination, WriteMode, WriteParams};
use lance::index::DatasetIndexExt;
use lance_file::version::LanceFileVersion;
use lance_select::RowAddrTreeMap;
use lance_table::format::{Fragment, IndexMetadata, RowIdMeta};
use lance_table::rowids::{RowIdSequence, write_row_ids};
use roaring::RoaringTreemap;

use crate::dataset::{open, store_params};
use crate::error::{EResult, EngineError};
use crate::rt::session;
use crate::types::conform_batch;

/// The C ABI's handle: the state behind a lock, so that a task the server
/// cancelled releases it before the next call takes it.
pub struct LeWrite {
    pub inner: Arc<tokio::sync::Mutex<WriteState>>,
}

/// Fragment ids a scan of the transaction's own fragments gives them:
/// unused by any committed fragment, unique in the process.  The fragments
/// themselves keep id 0, which Lance replaces at commit.
static TEMP_FRAGMENT_ID: AtomicU64 = AtomicU64::new(0xF000_0000);

struct PendingBatch {
    batch: RecordBatch,
    row_ids: Vec<u64>,
}

pub struct PendingFragment {
    pub fragment: Fragment,
    pub temp_id: u64,
    pub row_ids: Vec<u64>,
    min_id: u64,
    max_id: u64,
}

impl PendingFragment {
    fn position(&self, row_id: u64) -> Option<usize> {
        if row_id < self.min_id || row_id > self.max_id {
            return None;
        }
        // Row ids within a fragment are in the order the rows arrived,
        // which is ascending for one table in one backend.
        match self.row_ids.binary_search(&row_id) {
            Ok(i) => Some(i),
            Err(_) => self.row_ids.iter().position(|&r| r == row_id),
        }
    }

    pub fn scan_fragment(&self) -> Fragment {
        let mut f = self.fragment.clone();
        f.id = self.temp_id;
        f
    }
}

#[derive(Clone)]
struct Group {
    cid: i64,
    subxid: u32,
    rows: RoaringTreemap,
}

enum IndexOp {
    Create {
        meta: IndexMetadata,
        read_version: u64,
        replace: bool,
        subxid: u32,
    },
    Drop {
        name: String,
        subxid: u32,
    },
}

pub struct Spec {
    pub max_rows_per_file: usize,
    pub max_rows_per_group: usize,
    pub buffer_rows: usize,
    pub buffer_bytes: usize,
    pub storage_version: Option<LanceFileVersion>,
}

pub struct WriteState {
    pub uri: String,
    pub options: Arc<HashMap<String, String>>,
    /// The dataset the fragments are written into: its schema's field ids
    base: Arc<Dataset>,
    /// The schema the fragments are written in: the base's
    target_schema: SchemaRef,
    spec: Spec,
    buffer: Vec<PendingBatch>,
    buffered_rows: usize,
    buffered_bytes: usize,
    pub fragments: Vec<PendingFragment>,
    inserts: Vec<Group>,
    deletes: Vec<Group>,
    /// Rows appended by subtransactions that rolled back
    aborted: RoaringTreemap,
    /// TRUNCATE in this transaction: the command and subtransaction of each
    truncates: Vec<(i64, u32)>,
    index_ops: Vec<IndexOp>,
    /// Files written that no committed version references yet
    written: Vec<String>,
}

/// What a scan of the transaction's own view adds and hides.
pub struct OwnView {
    /// The transaction's fragments, under their temporary ids
    pub fragments: Vec<Fragment>,
    /// Row ids the scan's output must not show
    pub hide: RoaringTreemap,
    /// Committed rows the transaction deleted before the scan's command
    pub deleted_committed: RoaringTreemap,
    /// A TRUNCATE before the scan's command: no committed row is visible
    pub truncated: bool,
}

pub enum RowState {
    None,
    Appended,
    Deleted(i64),
}

fn add_to_groups(groups: &mut Vec<Group>, cid: i64, subxid: u32, ids: impl IntoIterator<Item = u64>) {
    if let Some(last) = groups.last_mut()
        && last.cid == cid
        && last.subxid == subxid
    {
        last.rows.extend(ids);
        return;
    }
    let mut rows = RoaringTreemap::new();
    rows.extend(ids);
    groups.push(Group { cid, subxid, rows });
}

impl WriteState {
    pub fn new(
        uri: &str,
        options: Arc<HashMap<String, String>>,
        base: Arc<Dataset>,
        spec: Spec,
    ) -> EResult<Self> {
        if !base.manifest().uses_stable_row_ids() {
            return Err(EngineError::unsupported(format!(
                "the dataset at {uri} has no stable row ids, so PostgreSQL cannot write it"
            )));
        }
        let target_schema = Arc::new(ArrowSchema::from(base.schema()));
        Ok(Self {
            uri: uri.to_string(),
            options,
            base,
            target_schema,
            spec,
            buffer: Vec::new(),
            buffered_rows: 0,
            buffered_bytes: 0,
            fragments: Vec::new(),
            inserts: Vec::new(),
            deletes: Vec::new(),
            aborted: RoaringTreemap::new(),
            truncates: Vec::new(),
            index_ops: Vec::new(),
            written: Vec::new(),
        })
    }

    pub fn has_changes(&self) -> bool {
        !self.buffer.is_empty()
            || !self.fragments.is_empty()
            || self.deletes.iter().any(|g| !g.rows.is_empty())
            || !self.truncates.is_empty()
            || !self.index_ops.is_empty()
    }

    fn is_pending(&self, row_id: u64) -> bool {
        self.inserts.iter().any(|g| g.rows.contains(row_id))
    }

    fn delete_cid(&self, row_id: u64) -> Option<i64> {
        self.deletes
            .iter()
            .filter(|g| g.rows.contains(row_id))
            .map(|g| g.cid)
            .min()
    }

    fn last_truncate(&self) -> Option<i64> {
        self.truncates.iter().map(|t| t.0).max()
    }

    pub fn row_state(&self, row_id: u64) -> RowState {
        if let Some(cid) = self.delete_cid(row_id) {
            return RowState::Deleted(cid);
        }
        if self.aborted.contains(row_id) {
            return RowState::Deleted(-1);
        }
        if self.is_pending(row_id) {
            return RowState::Appended;
        }
        RowState::None
    }

    pub async fn append(
        &mut self,
        batch: RecordBatch,
        row_ids: Vec<u64>,
        cid: i64,
        subxid: u32,
    ) -> EResult<()> {
        if batch.num_rows() != row_ids.len() {
            return Err(EngineError::invalid(format!(
                "a batch of {} rows came with {} row ids",
                batch.num_rows(),
                row_ids.len()
            )));
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch = conform_batch(&batch, &self.target_schema)?;
        add_to_groups(&mut self.inserts, cid, subxid, row_ids.iter().copied());
        self.buffered_rows += batch.num_rows();
        self.buffered_bytes += batch.get_array_memory_size();
        self.buffer.push(PendingBatch { batch, row_ids });
        if self.buffered_rows >= self.spec.buffer_rows || self.buffered_bytes >= self.spec.buffer_bytes {
            self.flush().await?;
        }
        Ok(())
    }

    pub fn delete(&mut self, row_ids: &[u64], cid: i64, subxid: u32) {
        add_to_groups(&mut self.deletes, cid, subxid, row_ids.iter().copied());
    }

    pub fn truncate(&mut self, cid: i64, subxid: u32) {
        self.truncates.push((cid, subxid));
    }

    pub fn subxact(&mut self, subxid: u32, parent: u32, commit: bool) {
        if commit {
            for g in self.inserts.iter_mut().chain(self.deletes.iter_mut()) {
                if g.subxid == subxid {
                    g.subxid = parent;
                }
            }
            for t in self.truncates.iter_mut() {
                if t.1 == subxid {
                    t.1 = parent;
                }
            }
            for op in self.index_ops.iter_mut() {
                match op {
                    IndexOp::Create { subxid: s, .. } | IndexOp::Drop { subxid: s, .. } => {
                        if *s == subxid {
                            *s = parent;
                        }
                    }
                }
            }
        } else {
            let mut aborted = RoaringTreemap::new();
            for g in self.inserts.iter().filter(|g| g.subxid == subxid) {
                aborted |= &g.rows;
            }
            self.aborted |= aborted;
            self.inserts.retain(|g| g.subxid != subxid);
            self.deletes.retain(|g| g.subxid != subxid);
            self.truncates.retain(|t| t.1 != subxid);
            self.index_ops.retain(|op| match op {
                IndexOp::Create { subxid: s, .. } | IndexOp::Drop { subxid: s, .. } => *s != subxid,
            });
        }
    }

    fn write_params(&self) -> EResult<WriteParams> {
        Ok(WriteParams {
            mode: WriteMode::Append,
            max_rows_per_file: self.spec.max_rows_per_file,
            max_rows_per_group: self.spec.max_rows_per_group,
            store_params: Some(store_params(&self.options)),
            session: Some(session()?),
            data_storage_version: self.spec.storage_version,
            skip_auto_cleanup: true,
            ..Default::default()
        })
    }

    /// Write batches as fragments of the base, not committed, giving each
    /// fragment its rows' ids.
    async fn write_fragments(&mut self, batches: Vec<RecordBatch>, row_ids: Vec<u64>) -> EResult<()> {
        if batches.is_empty() {
            return Ok(());
        }
        let params = self.write_params()?;
        crate::trace!("write_fragments: {} batches, {} rows", batches.len(), row_ids.len());
        let tx = InsertBuilder::new(WriteDestination::Dataset(self.base.clone()))
            .with_params(&params)
            .execute_uncommitted(batches)
            .await?;
        crate::trace!("write_fragments: written");
        let Operation::Append { fragments } = tx.operation else {
            return Err(EngineError::internal(
                "an uncommitted append did not produce an Append operation",
            ));
        };
        let mut next = 0usize;
        for mut fragment in fragments {
            let n = fragment
                .physical_rows
                .ok_or_else(|| EngineError::internal("a written fragment has no row count"))?;
            if next + n > row_ids.len() {
                return Err(EngineError::internal(
                    "the written fragments hold more rows than were given",
                ));
            }
            let ids = row_ids[next..next + n].to_vec();
            next += n;
            let sequence = RowIdSequence::from(ids.as_slice());
            fragment.row_id_meta = Some(RowIdMeta::Inline(write_row_ids(&sequence).into()));
            fragment.id = 0;
            for f in &fragment.files {
                self.written.push(format!("data/{}", f.path));
            }
            let min_id = ids.iter().copied().min().unwrap_or(0);
            let max_id = ids.iter().copied().max().unwrap_or(0);
            let mut sorted = ids;
            if !sorted.is_sorted() {
                // Keep the order of the rows; the lookup falls back to a scan.
                sorted.dedup();
            }
            self.fragments.push(PendingFragment {
                fragment,
                temp_id: TEMP_FRAGMENT_ID.fetch_add(1, Ordering::Relaxed),
                row_ids: sorted,
                min_id,
                max_id,
            });
        }
        if next != row_ids.len() {
            return Err(EngineError::internal(
                "the written fragments hold fewer rows than were given",
            ));
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> EResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let buffer = std::mem::take(&mut self.buffer);
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        let mut batches = Vec::with_capacity(buffer.len());
        let mut row_ids = Vec::new();
        for b in buffer {
            row_ids.extend_from_slice(&b.row_ids);
            batches.push(b.batch);
        }
        self.write_fragments(batches, row_ids).await
    }

    /// The view a scan of command `cid` has of the transaction's changes;
    /// the buffer is written out first, so that its rows are in fragments.
    pub async fn view(&mut self, cid: i64) -> EResult<OwnView> {
        self.flush().await?;
        if cid < 0 {
            // SnapshotAny's: every row version there is, none deleted.
            return Ok(OwnView {
                fragments: self.fragments.iter().map(|f| f.scan_fragment()).collect(),
                hide: self.aborted.clone(),
                deleted_committed: RoaringTreemap::new(),
                truncated: false,
            });
        }
        let truncate = self.truncates.iter().filter(|t| t.0 < cid).map(|t| t.0).max();
        let mut hide = self.aborted.clone();
        for g in &self.inserts {
            if g.cid >= cid || truncate.is_some_and(|t| g.cid < t) {
                hide |= &g.rows;
            }
        }
        let mut deleted = RoaringTreemap::new();
        for g in &self.deletes {
            if g.cid < cid {
                deleted |= &g.rows;
            }
        }
        hide |= &deleted;
        Ok(OwnView {
            fragments: self.fragments.iter().map(|f| f.scan_fragment()).collect(),
            hide,
            deleted_committed: deleted,
            truncated: truncate.is_some(),
        })
    }

    /// Where a row the transaction appended lies: its fragment and offset.
    pub fn locate(&self, row_id: u64) -> Option<(usize, usize)> {
        for (i, f) in self.fragments.iter().enumerate() {
            if let Some(pos) = f.position(row_id) {
                return Some((i, pos));
            }
        }
        None
    }

    pub fn base(&self) -> Arc<Dataset> {
        self.base.clone()
    }

    pub async fn create_index(
        &mut self,
        base: Arc<Dataset>,
        column: &str,
        index_type: &str,
        params_json: Option<&str>,
        name: Option<&str>,
        replace: bool,
        subxid: u32,
    ) -> EResult<()> {
        let meta = crate::maint::build_index(base.clone(), column, index_type, params_json, name, replace).await?;
        self.written.push(format!("_indices/{}", meta.uuid));
        self.index_ops.push(IndexOp::Create {
            meta,
            read_version: base.version_id(),
            replace,
            subxid,
        });
        Ok(())
    }

    pub fn drop_index(&mut self, name: &str, subxid: u32) {
        self.index_ops.push(IndexOp::Drop {
            name: name.to_string(),
            subxid,
        });
    }

    /// The rows of a pending fragment that survive: those not rolled back,
    /// not deleted, and not before a TRUNCATE.
    fn dead_rows(&self) -> RoaringTreemap {
        let mut dead = self.aborted.clone();
        for g in &self.deletes {
            for id in g.rows.iter() {
                if self.is_pending(id) {
                    dead.insert(id);
                }
            }
        }
        if let Some(t) = self.last_truncate() {
            for g in &self.inserts {
                if g.cid < t {
                    dead |= &g.rows;
                }
            }
        }
        dead
    }

    /// Rewrite the pending fragments that hold dead rows without them.
    async fn drop_dead_rows(&mut self) -> EResult<Vec<Fragment>> {
        let dead = self.dead_rows();
        let fragments = std::mem::take(&mut self.fragments);
        let mut keep = Vec::new();
        let mut rewrite_batches = Vec::new();
        let mut rewrite_ids = Vec::new();
        for pf in fragments {
            if !pf.row_ids.iter().any(|id| dead.contains(*id)) {
                keep.push(pf);
                continue;
            }
            let mut scanner = self.base.scan();
            scanner.with_fragments(vec![pf.scan_fragment()]);
            scanner.with_row_id();
            scanner.scan_in_order(true);
            let mut stream = scanner.try_into_stream().await?;
            while let Some(batch) = stream.try_next().await? {
                let ids = batch
                    .column_by_name(lance_core::ROW_ID)
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>().cloned())
                    .ok_or_else(|| EngineError::internal("a scan of a pending fragment has no _rowid"))?;
                let mask: BooleanArray = ids.iter().map(|id| Some(!dead.contains(id.unwrap_or(0)))).collect();
                let filtered = arrow_select::filter::filter_record_batch(&batch, &mask)?;
                if filtered.num_rows() == 0 {
                    continue;
                }
                let kept_ids = filtered
                    .column_by_name(lance_core::ROW_ID)
                    .and_then(|c| c.as_any().downcast_ref::<UInt64Array>().cloned())
                    .ok_or_else(|| EngineError::internal("lost the _rowid column"))?;
                rewrite_ids.extend(kept_ids.values().iter().copied());
                let idx = filtered.schema().index_of(lance_core::ROW_ID)?;
                let mut filtered = filtered;
                filtered.remove_column(idx);
                rewrite_batches.push(conform_batch(&filtered, &self.target_schema)?);
            }
        }
        self.fragments = keep;
        if !rewrite_batches.is_empty() {
            self.write_fragments(rewrite_batches, rewrite_ids).await?;
        }
        Ok(self.fragments.iter().map(|f| f.fragment.clone()).collect())
    }

    /// The deletion vectors of the committed rows the transaction deleted,
    /// on `latest`: the fragments changed, those emptied, and the rows'
    /// addresses.  A row `latest` no longer has live is a conflict: another
    /// transaction deleted or updated it first.
    async fn deletions_on(
        &self,
        latest: &Arc<Dataset>,
    ) -> EResult<(Vec<Fragment>, Vec<u64>, RowAddrTreeMap, u64)> {
        let mut committed = RoaringTreemap::new();
        for g in &self.deletes {
            for id in g.rows.iter() {
                if !self.is_pending(id) {
                    committed.insert(id);
                }
            }
        }
        if committed.is_empty() {
            return Ok((Vec::new(), Vec::new(), RowAddrTreeMap::new(), 0));
        }
        let index = get_row_id_index(latest)
            .await?
            .ok_or_else(|| EngineError::internal("the dataset has no row id index"))?;
        let mut by_fragment: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        let mut addrs = RowAddrTreeMap::new();
        for id in committed.iter() {
            let Some(addr) = index.get(id)? else {
                return Err(EngineError::conflict(format!(
                    "could not serialize access due to concurrent update: row {id} was deleted or updated by another transaction"
                )));
            };
            by_fragment
                .entry(addr.fragment_id())
                .or_default()
                .push(addr.row_offset());
            addrs.insert(u64::from(addr));
        }
        let mut updated = Vec::new();
        let mut removed = Vec::new();
        for (fragment_id, offsets) in by_fragment {
            let meta = latest
                .manifest()
                .fragments
                .iter()
                .find(|f| f.id == fragment_id as u64)
                .cloned()
                .ok_or_else(|| EngineError::conflict(format!("fragment {fragment_id} was replaced concurrently")))?;
            let ff = FileFragment::new(latest.clone(), meta);
            if let Some(dv) = ff.get_deletion_vector().await?
                && offsets.iter().any(|o| dv.contains(*o))
            {
                return Err(EngineError::conflict(
                    "could not serialize access due to concurrent delete",
                ));
            }
            match ff.extend_deletions(offsets).await? {
                Some(f) => updated.push(f.metadata().clone()),
                None => removed.push(fragment_id as u64),
            }
        }
        Ok((updated, removed, addrs, committed.len()))
    }

    /// Commit the transaction's changes on `latest`, returning the last
    /// version committed.
    pub async fn commit(
        &mut self,
        latest: Arc<Dataset>,
        properties: HashMap<String, String>,
    ) -> EResult<u64> {
        self.flush().await?;
        let new_fragments = self.drop_dead_rows().await?;
        let props = Some(Arc::new(properties.clone()));
        let read_version = latest.version_id();
        let mut current = latest.clone();

        let truncated = self.last_truncate().is_some();
        let operation = if truncated {
            Some(Operation::Overwrite {
                fragments: new_fragments,
                schema: current.schema().clone(),
                config_upsert_values: None,
                initial_bases: None,
            })
        } else {
            let (updated, removed, affected, ndeleted) = self.deletions_on(&current).await?;
            if ndeleted == 0 {
                if new_fragments.is_empty() {
                    None
                } else {
                    Some(Operation::Append {
                        fragments: new_fragments,
                    })
                }
            } else {
                let op = Operation::Update {
                    removed_fragment_ids: removed,
                    updated_fragments: updated,
                    new_fragments,
                    fields_modified: vec![],
                    compacted_sstables: vec![],
                    fields_for_preserving_frag_bitmap: vec![],
                    update_mode: Some(UpdateMode::RewriteRows),
                    inserted_rows_filter: None,
                    updated_fragment_offsets: None,
                };
                let tx = TransactionBuilder::new(read_version, op)
                    .transaction_properties(props.clone())
                    .build();
                let ds = CommitBuilder::new(current.clone())
                    .with_session(session()?)
                    .with_affected_rows(affected)
                    .with_skip_auto_cleanup(true)
                    .execute(tx)
                    .await?;
                current = Arc::new(ds);
                None
            }
        };
        if let Some(op) = operation {
            let tx = TransactionBuilder::new(current.version_id(), op)
                .transaction_properties(props.clone())
                .build();
            let ds = CommitBuilder::new(current.clone())
                .with_session(session()?)
                .with_skip_auto_cleanup(true)
                .execute(tx)
                .await?;
            current = Arc::new(ds);
        }

        let index_ops = std::mem::take(&mut self.index_ops);
        for op in index_ops {
            let (new_indices, removed_indices, read_version) = match op {
                IndexOp::Create {
                    meta,
                    read_version,
                    replace,
                    ..
                } => {
                    let existing = current.load_indices().await?;
                    let removed: Vec<IndexMetadata> = existing
                        .iter()
                        .filter(|i| i.name == meta.name)
                        .cloned()
                        .collect();
                    if !removed.is_empty() && !replace {
                        return Err(EngineError::new(
                            crate::error::LE_ERR_EXISTS,
                            format!("the dataset already has an index named \"{}\"", meta.name),
                        ));
                    }
                    (vec![meta], removed, read_version)
                }
                IndexOp::Drop { name, .. } => {
                    let existing = current.load_indices().await?;
                    let removed: Vec<IndexMetadata> =
                        existing.iter().filter(|i| i.name == name).cloned().collect();
                    if removed.is_empty() {
                        continue;
                    }
                    (vec![], removed, current.version_id())
                }
            };
            let tx = TransactionBuilder::new(
                read_version,
                Operation::CreateIndex {
                    new_indices,
                    removed_indices,
                },
            )
            .transaction_properties(props.clone())
            .build();
            let ds = CommitBuilder::new(current.clone())
                .with_session(session()?)
                .with_skip_auto_cleanup(true)
                .execute(tx)
                .await?;
            current = Arc::new(ds);
        }
        self.written.clear();
        self.inserts.clear();
        self.deletes.clear();
        self.truncates.clear();
        Ok(current.version_id())
    }

    /// Remove the files the transaction wrote that nothing committed.
    pub async fn cleanup(&mut self) {
        if self.written.is_empty() {
            return;
        }
        let Ok(registry) = session().map(|s| s.store_registry()) else {
            return;
        };
        let params = store_params(&self.options);
        let Ok((store, base)) =
            lance::io::ObjectStore::from_uri_and_params(registry, &self.uri, &params).await
        else {
            return;
        };
        for rel in std::mem::take(&mut self.written) {
            let path = object_store::path::Path::from(format!("{base}/{rel}"));
            if rel.starts_with("_indices/") {
                let _ = store.remove_dir_all(path).await;
            } else {
                let _ = store.delete(&path).await;
            }
        }
    }
}

/// Open the base a transaction's first write is made on.
pub async fn begin(
    uri: &str,
    options: Arc<HashMap<String, String>>,
    base: Option<Arc<Dataset>>,
    spec: Spec,
) -> EResult<WriteState> {
    let base = match base {
        Some(b) => b,
        None => open(uri, &options, None).await?,
    };
    WriteState::new(uri, options, base, spec)
}
