use mongodb::bson::Document;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::models::v1::{
    Dataset, DatasetVersion, Metadata, Object, ObjectGroup, Project,
};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

pub trait HasMetadata {
    fn meta_data(&self) -> Vec<MetaDataEntry>;
    fn resource_id(&self) -> &str;
}

fn map_metadata(
    resource_id: &str,
    object_type: ObjectType,
    md: &Vec<Metadata>,
) -> Vec<MetaDataEntry> {
    md.iter()
        .filter_map(|m| {
            // We only support json metadata for now
            serde_json::from_slice(&m.metadata)
                .ok()
                .map(|json| MetaDataEntry {
                    resource_id: resource_id.to_string(),
                    resource_type: object_type.clone(),
                    key: m.key.to_string(),
                    labels: m
                        .labels
                        .iter()
                        .map(|l| Label::new(&l.key, &l.value))
                        .collect(),
                    metadata: json,
                })
        })
        .collect::<Vec<_>>()
}

impl HasMetadata for Project {
    fn meta_data(&self) -> Vec<MetaDataEntry> {
        map_metadata(&self.id, ObjectType::Project, &self.metadata)
    }

    fn resource_id(&self) -> &str {
        &self.id
    }
}

impl HasMetadata for Dataset {
    fn meta_data(&self) -> Vec<MetaDataEntry> {
        map_metadata(&self.id, ObjectType::Dataset, &self.metadata)
    }

    fn resource_id(&self) -> &str {
        &self.id
    }
}

impl HasMetadata for DatasetVersion {
    fn meta_data(&self) -> Vec<MetaDataEntry> {
        map_metadata(&self.id, ObjectType::DatasetVersion, &self.metadata)
    }

    fn resource_id(&self) -> &str {
        &self.id
    }
}

impl HasMetadata for ObjectGroup {
    fn meta_data(&self) -> Vec<MetaDataEntry> {
        map_metadata(&self.id, ObjectType::ObjectGroup, &self.metadata)
    }

    fn resource_id(&self) -> &str {
        &self.id
    }
}

impl HasMetadata for Object {
    fn meta_data(&self) -> Vec<MetaDataEntry> {
        map_metadata(&self.id, ObjectType::Object, &self.metadata)
    }

    fn resource_id(&self) -> &str {
        &self.id
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum ObjectType {
    Object,
    ObjectGroup,
    Dataset,
    DatasetVersion,
    Project,
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Object => f.write_str("Object"),
            Self::ObjectGroup => f.write_str("ObjectGroup"),
            Self::Dataset => f.write_str("Dataset"),
            Self::DatasetVersion => f.write_str("DatasetVersion"),
            Self::Project => f.write_str("Project"),
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
    pub resource_id: String,
    pub resource_type: ObjectType,
    pub key: String,
    pub labels: Vec<Label>,
    pub metadata: Document,
}
