use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MongoSettings {
    pub servers: Vec<String>,
    pub database: String,
    pub collection: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LogSettings {
    pub level: String,
}
