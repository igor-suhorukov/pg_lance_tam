// SPDX-License-Identifier: Apache-2.0
//! Errors as the C ABI reports them (include/lance_engine.h, LE_ERR_*).

use std::fmt;

pub const LE_OK: i32 = 0;
pub const LE_ERR_INTERNAL: i32 = 1;
pub const LE_ERR_INVALID: i32 = 2;
pub const LE_ERR_NOT_FOUND: i32 = 3;
pub const LE_ERR_CONFLICT: i32 = 4;
pub const LE_ERR_INTERRUPTED: i32 = 5;
pub const LE_ERR_IO: i32 = 6;
pub const LE_ERR_PANIC: i32 = 7;
pub const LE_ERR_UNSUPPORTED: i32 = 8;
pub const LE_ERR_EXISTS: i32 = 9;

#[derive(Debug, Clone)]
pub struct EngineError {
    pub code: i32,
    pub message: String,
}

pub type EResult<T> = std::result::Result<T, EngineError>;

impl EngineError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(LE_ERR_INVALID, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(LE_ERR_INTERNAL, message)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(LE_ERR_CONFLICT, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(LE_ERR_NOT_FOUND, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(LE_ERR_UNSUPPORTED, message)
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<lance::Error> for EngineError {
    fn from(e: lance::Error) -> Self {
        use lance::Error as E;
        let code = match &e {
            E::InvalidInput { .. }
            | E::SchemaMismatch { .. }
            | E::Schema { .. }
            | E::InvalidTableLocation { .. }
            | E::FieldNotFound { .. } => LE_ERR_INVALID,
            E::DatasetNotFound { .. }
            | E::NotFound { .. }
            | E::IndexNotFound { .. }
            | E::VersionNotFound { .. }
            | E::RefNotFound { .. } => LE_ERR_NOT_FOUND,
            E::DatasetAlreadyExists { .. } => LE_ERR_EXISTS,
            E::CommitConflict { .. }
            | E::IncompatibleTransaction { .. }
            | E::RetryableCommitConflict { .. }
            | E::TooMuchWriteContention { .. }
            | E::VersionConflict { .. } => LE_ERR_CONFLICT,
            E::NotSupported { .. } => LE_ERR_UNSUPPORTED,
            E::IO { .. } | E::CorruptFile { .. } => LE_ERR_IO,
            _ => LE_ERR_INTERNAL,
        };
        Self::new(code, e.to_string())
    }
}

impl From<arrow_schema::ArrowError> for EngineError {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::new(LE_ERR_INVALID, format!("arrow: {e}"))
    }
}

impl From<datafusion::error::DataFusionError> for EngineError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        Self::new(LE_ERR_INTERNAL, format!("datafusion: {e}"))
    }
}

impl From<serde_json::Error> for EngineError {
    fn from(e: serde_json::Error) -> Self {
        Self::new(LE_ERR_INVALID, format!("json: {e}"))
    }
}

impl From<std::str::Utf8Error> for EngineError {
    fn from(e: std::str::Utf8Error) -> Self {
        Self::new(LE_ERR_INVALID, format!("utf-8: {e}"))
    }
}
