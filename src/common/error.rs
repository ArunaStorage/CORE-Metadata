use crate::data::ObjectType;
use std::fmt::Debug;
use tokio::sync::mpsc::error::SendError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    LogParse {
        source: log::ParseLevelError,
    },
    Log {
        source: flexi_logger::FlexiLoggerError,
    },
    Mongo {
        source: mongodb::error::Error,
    },
    SerdeJson {
        source: serde_json::Error,
    },
    AddrParse {
        source: std::net::AddrParseError,
    },
    Tonic {
        source: Box<dyn std::error::Error>,
    },
    QueryParse {
        source: mongodb::bson::ser::Error,
    },
    Tokio {
        source: Box<dyn std::error::Error>,
    },
    Query,

    RsourceNotFound {
        id: String,
        resource_type: ObjectType,
    },

    UnknownResourceType {
        resource_type: i32,
    },

    MessageProcessing,
}

impl From<log::ParseLevelError> for Error {
    fn from(source: log::ParseLevelError) -> Self {
        Self::LogParse { source }
    }
}

impl From<flexi_logger::FlexiLoggerError> for Error {
    fn from(source: flexi_logger::FlexiLoggerError) -> Self {
        Self::Log { source }
    }
}

impl From<mongodb::error::Error> for Error {
    fn from(source: mongodb::error::Error) -> Self {
        Self::Mongo { source }
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Self::SerdeJson { source }
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(source: std::net::AddrParseError) -> Self {
        Self::AddrParse { source }
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(source: tonic::transport::Error) -> Self {
        Self::Tonic {
            source: Box::new(source),
        }
    }
}

impl From<tonic::Status> for Error {
    fn from(source: tonic::Status) -> Self {
        Self::Tonic {
            source: Box::new(source),
        }
    }
}

impl From<tonic::codegen::http::uri::InvalidUri> for Error {
    fn from(source: tonic::codegen::http::uri::InvalidUri) -> Self {
        Self::Tonic {
            source: Box::new(source),
        }
    }
}

impl From<tonic::metadata::errors::InvalidMetadataValue> for Error {
    fn from(source: tonic::metadata::errors::InvalidMetadataValue) -> Self {
        Self::Tonic {
            source: Box::new(source),
        }
    }
}

//
// impl From<mongodb::bson::de::Error> for Error {
//     fn from(source: mongodb::bson::de::Error) -> Self {
//         Self::QueryParseError { source }
//     }
// }

impl From<mongodb::bson::ser::Error> for Error {
    fn from(source: mongodb::bson::ser::Error) -> Self {
        Self::QueryParse { source }
    }
}

impl<T: 'static + Debug> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(source: SendError<T>) -> Self {
        Self::Tokio {
            source: Box::new(source),
        }
    }
}
