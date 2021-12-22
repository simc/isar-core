use crate::mdbx::error::MdbxError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, IsarError>;

#[derive(Debug, Error, Eq, PartialEq)]
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

    #[error("Auto increment id cannot be generated because the limit is reached.")]
    AutoIncrementOverflow {},

    #[error("The provided ObjectId does not match the collection.")]
    InvalidObjectId {},

    #[error("The provided object is invalid.")]
    InvalidObject {},

    #[error("Transaction closed.")]
    TransactionClosed {},

    #[error("IllegalArg: {message:?}.")]
    IllegalArg { message: String },

    #[error("Index could not be found.")]
    UnknownIndex {},

    #[error("Invalid JSON.")]
    InvalidJson {},

    #[error("DbCorrupted: {message:?}")]
    DbCorrupted { message: String },

    #[error("SchemaError: {message:?}")]
    SchemaError { message: String },

    #[error("InstanceMismatch: The transaction is from a different instance.")]
    InstanceMismatch {},

    #[error("LmdbError ({code:?}): {message:?}")]
    LmdbError { code: i32, message: String },
}

impl IsarError {}

impl From<MdbxError> for IsarError {
    fn from(e: MdbxError) -> Self {
        match e {
            MdbxError::MapFull {} => IsarError::DbFull {},
            MdbxError::Other { code, message } => IsarError::LmdbError { code, message },
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

pub fn schema_error<T>(msg: &str) -> Result<T> {
    Err(IsarError::SchemaError {
        message: msg.to_string(),
    })
}
