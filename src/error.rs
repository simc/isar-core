use crate::lmdb::error::LmdbError;
use std::error::Error;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, IsarError>;

#[derive(Debug, Error)]
pub enum IsarError {
    #[error("Isar version of the file is too new or too old to be used.")]
    VersionError {},

    #[error("No such file or directory. Please make sure that the provided path is valid.")]
    PathError {},

    #[error("The database is full.")]
    DbFull {},

    #[error("Unique index violated.")]
    UniqueViolated {},

    #[error("Write transaction required.")]
    WriteTxnRequired {},

    #[error("The provided ObjectId is invalid.")]
    InvalidObjectId {},

    #[error("The provided object is invalid.")]
    InvalidObject {},

    #[error("Transaction closed.")]
    TransactionClosed {},

    #[error("IllegalArg: {message:?}.")]
    IllegalArg { message: String },

    #[error("Invalid JSON.")]
    InvalidJson {},

    #[error("DbCorrupted: {message:?}")]
    DbCorrupted {
        source: Option<Box<dyn Error>>,
        message: String,
    },

    #[error("MigrationError: {message:?}")]
    MigrationError {
        source: Option<Box<dyn Error>>,
        message: String,
    },

    #[error("LmdbError ({code:?}): {message:?}")]
    LmdbError { code: i32, message: String },
}

impl IsarError {}

impl From<LmdbError> for IsarError {
    fn from(e: LmdbError) -> Self {
        match e {
            LmdbError::MapFull {} => IsarError::DbFull {},
            LmdbError::Other { code, message } => IsarError::LmdbError { code, message },
            _ => IsarError::LmdbError {
                code: e.to_err_code(),
                message: "Error that should have been catched.".to_string(),
            },
        }
    }
}

pub fn illegal_arg<T>(msg: &str) -> Result<T> {
    Err(IsarError::IllegalArg {
        message: msg.to_string(),
    })
}
