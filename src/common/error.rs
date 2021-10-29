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
    Kafka {
        source: rdkafka::error::KafkaError,
    },
    SerdeJson {
        source: serde_json::Error,
    },
    AddrParse {
        source: std::net::AddrParseError,
    },
    TonicTransport {
        source: tonic::transport::Error,
    },
    QueryParse {
        source: mongodb::bson::ser::Error,
    },
    Query,
}

impl From<log::ParseLevelError> for Error {
    fn from(source: log::ParseLevelError) -> Self {
        Error::LogParse { source }
    }
}

impl From<flexi_logger::FlexiLoggerError> for Error {
    fn from(source: flexi_logger::FlexiLoggerError) -> Self {
        Error::Log { source }
    }
}

impl From<mongodb::error::Error> for Error {
    fn from(source: mongodb::error::Error) -> Self {
        Error::Mongo { source }
    }
}

impl From<rdkafka::error::KafkaError> for Error {
    fn from(source: rdkafka::error::KafkaError) -> Self {
        Error::Kafka { source }
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Error::SerdeJson { source }
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(source: std::net::AddrParseError) -> Self {
        Error::AddrParse { source }
    }
}

impl From<tonic::transport::Error> for Error {
    fn from(source: tonic::transport::Error) -> Self {
        Error::TonicTransport { source }
    }
}
//
// impl From<mongodb::bson::de::Error> for Error {
//     fn from(source: mongodb::bson::de::Error) -> Self {
//         Error::QueryParseError { source }
//     }
// }

impl From<mongodb::bson::ser::Error> for Error {
    fn from(source: mongodb::bson::ser::Error) -> Self {
        Error::QueryParse { source }
    }
}
