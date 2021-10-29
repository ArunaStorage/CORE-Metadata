use mongodb::bson::Document;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum ObjectType {
    Object,
    ObjectGroup,
    Dataset,
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Object => f.write_str("Object"),
            Self::ObjectGroup => f.write_str("ObjectGroup"),
            Self::Dataset => f.write_str("Dataset"),
        }
    }
}

impl From<ObjectType> for mongodb::bson::Bson {
    fn from(v: ObjectType) -> Self {
        mongodb::bson::bson!(v.to_string())
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Label {
    pub key: String,
    pub value: String,
}

impl Label {
    pub fn new(key: &str, value: &str) -> Label {
        Label {
            key: key.to_string(),
            value: value.to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct MetaDataEntry {
    pub id: i64,
    pub object_type: ObjectType,
    pub key: String,
    pub labels: Vec<Label>,
    pub metadata: Document,
}
