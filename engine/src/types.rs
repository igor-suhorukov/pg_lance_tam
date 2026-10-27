// SPDX-License-Identifier: Apache-2.0
//! Batches cast to the types each side asks for: a write to the dataset's
//! schema, a scan to the types the extension reads (docs/design.md §2.5).

use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, ArrayRef, RecordBatch, StructArray, new_null_array};
use arrow_cast::cast::{CastOptions, can_cast_types, cast_with_options};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::error::{EResult, EngineError};

const CAST: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: arrow_cast::display::FormatOptions::new(),
};

pub fn cast_column(array: &ArrayRef, to: &DataType, name: &str) -> EResult<ArrayRef> {
    if array.data_type() == to {
        return Ok(array.clone());
    }
    if !can_cast_types(array.data_type(), to) {
        return Err(EngineError::invalid(format!(
            "column \"{name}\": cannot convert {} to {}",
            array.data_type(),
            to
        )));
    }
    cast_with_options(array, to, &CAST).map_err(|e| {
        EngineError::invalid(format!(
            "column \"{name}\": cannot convert {} to {to}: {e}",
            array.data_type()
        ))
    })
}

/// A batch as the dataset's schema has it: the columns by name, cast where
/// the types differ; a column of the dataset the batch lacks is NULL.
pub fn conform_batch(batch: &RecordBatch, target: &SchemaRef) -> EResult<RecordBatch> {
    if batch.schema().fields() == target.fields() {
        return Ok(RecordBatch::try_new(target.clone(), batch.columns().to_vec())?);
    }
    let rows = batch.num_rows();
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let column = match batch.column_by_name(field.name()) {
            Some(c) => cast_column(c, field.data_type(), field.name())?,
            None if field.is_nullable() => new_null_array(field.data_type(), rows),
            None => {
                return Err(EngineError::invalid(format!(
                    "the dataset's column \"{}\" is not nullable and was not given",
                    field.name()
                )));
            }
        };
        if !field.is_nullable() && column.null_count() > 0 {
            return Err(EngineError::invalid(format!(
                "null value in column \"{}\" of the dataset, which is not nullable",
                field.name()
            )));
        }
        columns.push(column);
    }
    Ok(RecordBatch::try_new(target.clone(), columns)?)
}

/// The columns of a scan's batch cast to the types asked for, in order,
/// followed by the extra columns (_rowid, _distance, _score) kept as they are.
pub fn cast_for_reader(batch: &RecordBatch, wanted: &[Field], extra: &[&str]) -> EResult<RecordBatch> {
    let mut fields = Vec::with_capacity(wanted.len() + extra.len());
    let mut columns = Vec::with_capacity(wanted.len() + extra.len());
    for (i, f) in wanted.iter().enumerate() {
        let column = batch
            .column_by_name(f.name())
            .cloned()
            .or_else(|| batch.columns().get(i).cloned())
            .ok_or_else(|| EngineError::internal(format!("the scan lost column \"{}\"", f.name())))?;
        let column = cast_column(&column, f.data_type(), f.name())?;
        fields.push(Field::new(f.name(), f.data_type().clone(), true));
        columns.push(column);
    }
    for name in extra {
        if let Some(c) = batch.column_by_name(name) {
            fields.push(Field::new(*name, c.data_type().clone(), true));
            columns.push(c.clone());
        }
    }
    Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?)
}

/// A batch exported through the C Data Interface, as one struct array.
///
/// # Safety
/// `array` and `schema` point at writable, uninitialized structs.
pub unsafe fn export_batch(
    batch: RecordBatch,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> EResult<()> {
    let struct_array = StructArray::from(batch);
    let data = struct_array.into_data();
    let ffi_schema = FFI_ArrowSchema::try_from(data.data_type())?;
    let ffi_array = FFI_ArrowArray::new(&data);
    unsafe {
        std::ptr::write(array, ffi_array);
        std::ptr::write(schema, ffi_schema);
    }
    Ok(())
}

/// A struct array the extension exported, imported as a batch: the array is
/// moved out of `array`, whose release callback is then this side's to call.
///
/// # Safety
/// `array` points at a valid exported array, `schema` at its schema.
pub unsafe fn import_batch(array: *mut FFI_ArrowArray, schema: *const FFI_ArrowSchema) -> EResult<RecordBatch> {
    if array.is_null() || schema.is_null() {
        return Err(EngineError::invalid("a batch is NULL"));
    }
    let ffi_array = unsafe { FFI_ArrowArray::from_raw(array) };
    let data = unsafe { arrow::ffi::from_ffi(ffi_array, &*schema) }?;
    let struct_array = StructArray::from(data);
    Ok(RecordBatch::from(struct_array))
}

/// The fields of an exported struct schema.
///
/// # Safety
/// `schema` is NULL or points at a valid exported schema.
pub unsafe fn import_fields(schema: *const FFI_ArrowSchema) -> EResult<Option<Vec<Field>>> {
    if schema.is_null() {
        return Ok(None);
    }
    let s = Schema::try_from(unsafe { &*schema })?;
    Ok(Some(s.fields().iter().map(|f| f.as_ref().clone()).collect()))
}

pub fn null_array(field: &Field, rows: usize) -> ArrayRef {
    new_null_array(field.data_type(), rows)
}
