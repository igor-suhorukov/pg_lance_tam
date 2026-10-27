// SPDX-License-Identifier: Apache-2.0
//! New columns of a dataset, for ALTER TABLE ... ADD COLUMN
//! (docs/design.md §2.15).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::Schema as ArrowSchema;
use lance::Dataset;
use lance::dataset::transaction::{Operation, TransactionBuilder};
use lance::dataset::{BatchUDF, CommitBuilder, NewColumnTransform};

use crate::error::EResult;
use crate::rt::session;

/// The columns of `fields` added to every fragment of `base`, each row the
/// dataset has taking the one value `values` holds for it (none: null), and
/// committed as one version tagged with `properties`.  The columns are
/// nullable in Lance: PostgreSQL keeps the table's NOT NULL constraints.
pub async fn add_columns(
    base: Arc<Dataset>,
    fields: ArrowSchema,
    values: Option<RecordBatch>,
    properties: HashMap<String, String>,
) -> EResult<u64> {
    let fields = Arc::new(ArrowSchema::new(
        fields
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ));
    let values = match values {
        Some(b) if b.columns().iter().any(|c| c.null_count() < c.len()) => Some(
            RecordBatch::try_new(fields.clone(), b.columns().to_vec())?,
        ),
        _ => None,
    };
    // a column of nulls is the manifest's alone; a value is written out, a
    // file a fragment, beside the narrowest column, whose batches it counts
    let read = base
        .schema()
        .fields
        .iter()
        .min_by_key(|f| f.data_type().primitive_width().unwrap_or(usize::MAX))
        .map(|f| vec![f.name.clone()]);
    let mut fragments = Vec::new();
    let mut schema = None;
    for fragment in base.get_fragments() {
        let transform = match &values {
            None => NewColumnTransform::AllNulls(fields.clone()),
            Some(values) => {
                let values = values.clone();
                NewColumnTransform::BatchUDF(BatchUDF {
                    mapper: Box::new(move |batch: &RecordBatch| {
                        let first = UInt32Array::from(vec![0u32; batch.num_rows()]);
                        let columns = values
                            .columns()
                            .iter()
                            .map(|c| arrow_select::take::take(c.as_ref(), &first, None))
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok(RecordBatch::try_new(values.schema(), columns)?)
                    }),
                    output_schema: fields.clone(),
                    result_checkpoint: None,
                })
            }
        };
        let (f, s) = fragment.add_columns(transform, read.clone(), None).await?;
        fragments.push(f);
        schema = Some(s);
    }
    let schema = match schema {
        Some(s) => s,
        None => {
            // no fragments: the schema alone, as add_columns makes it
            let mut s = base.schema().merge(fields.as_ref())?;
            s.try_set_field_id(Some(base.manifest().max_field_id()))?;
            s
        }
    };
    let tx = TransactionBuilder::new(
        base.version_id(),
        Operation::Merge {
            fragments,
            schema,
            preserves_nullability: true,
        },
    )
    .transaction_properties(Some(Arc::new(properties)))
    .build();
    let ds = CommitBuilder::new(base).with_session(session()?).execute(tx).await?;
    Ok(ds.version_id())
}

/// The columns named dropped from `base`, as one version tagged with
/// `properties`: the manifest's change alone, the data staying until
/// compaction rewrites the fragments.
pub async fn drop_columns(
    base: Arc<Dataset>,
    names: Vec<String>,
    properties: HashMap<String, String>,
) -> EResult<u64> {
    let columns = names.iter().map(String::as_str).collect::<Vec<_>>();
    let schema = base.plan_drop_columns(&columns)?;
    let tx = TransactionBuilder::new(
        base.version_id(),
        Operation::Project {
            schema,
            preserves_nullability: true,
        },
    )
    .transaction_properties(Some(Arc::new(properties)))
    .build();
    let ds = CommitBuilder::new(base).with_session(session()?).execute(tx).await?;
    Ok(ds.version_id())
}
