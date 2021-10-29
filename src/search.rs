use crate::common::settings::{LogSettings, MongoSettings};
use crate::common::{data, error, util};
use config::{Config, ConfigError, Environment, File};
use data::MetaDataEntry;
use futures::StreamExt;
use log::{debug, error, info, warn};
use mongodb::options::FindOptions;
use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};
use tonic::transport::Server;

mod common;

pub mod meta_search {
    include!(concat!(env!("OUT_DIR"), "/meta_search.rs"));
}

const DEFAULT_PAGE_SIZE: usize = 10;
const DEFAULT_PAGE: u32 = 1;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct GrpcSettings {
    pub bind_address: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Settings {
    pub mongo: MongoSettings,
    pub grpc: GrpcSettings,
    pub log: LogSettings,
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::default();

        // Start off by merging in the "default" configuration file
        s.merge(File::with_name("config.toml"))?;

        // Add in a local configuration file
        // This file shouldn't be checked in to git
        s.merge(File::with_name("config_local.toml").required(false))?;

        // Add in settings from the environment (with a prefix of LOADER)
        // Eg.. `LOADER_DEBUG=1 ./target/app` would set the `debug` key
        s.merge(Environment::with_prefix("loader"))?;

        s.try_into()
    }
}

#[derive(Debug)]
pub struct Searcher {
    collection: mongodb::Collection<data::MetaDataEntry>,
}

impl Searcher {
    async fn query_mongo(&self, spec: &SearchSpec) -> error::Result<meta_search::SearchReply> {
        // Count total results
        let count = self
            .collection
            .count_documents(spec.filter.clone(), None)
            .await?;

        let mut cursor = self
            .collection
            .find(spec.filter.clone(), spec.find_options())
            .await?;
        let mut results = Vec::with_capacity(spec.page_size);

        while let Some(entry) = cursor.next().await {
            results.push(entry?.into());
        }

        Ok(meta_search::SearchReply {
            results,
            pagination: Some(meta_search::search_reply::Pagination {
                page: spec.page,
                page_count: (count + spec.page_size as u64 - 1) / spec.page_size as u64,
                page_size: spec.page_size as u32,
                result_count: count,
            }),
        })
    }
}

#[derive(Debug)]
pub struct SearchSpec {
    filter: mongodb::bson::Document,
    page_size: usize,
    page: u32,
}

impl SearchSpec {
    pub fn find_options(&self) -> mongodb::options::FindOptions {
        let skip = (self.page - 1) as u64 * self.page_size as u64;
        FindOptions::builder()
            .sort(mongodb::bson::doc! { "id" : -1 })
            .skip(skip)
            .limit(self.page_size as i64)
            .build()
    }
}

impl TryFrom<meta_search::SearchRequest> for SearchSpec {
    type Error = error::Error;

    fn try_from(sr: meta_search::SearchRequest) -> Result<Self, Self::Error> {
        let mut doc = mongodb::bson::Document::new();

        if let Some(ot) = sr.object_type {
            if let Some(ot) = meta_search::ObjectType::from_i32(ot) {
                doc.insert("object_type", ot as i32);
            }
        };

        if let Some(key) = sr.key {
            doc.insert("key", key);
        }

        for fq in sr.conditions.iter() {
            debug!("Adding condition: {}: {}", &fq.key, &fq.query);
            let key = format!("metadata.{}", fq.key);
            let val: mongodb::bson::Bson = serde_json::from_str(fq.query.as_str())?;
            doc.insert(key, val);
        }

        // Compute limits
        let mut page = DEFAULT_PAGE;
        let mut page_size = DEFAULT_PAGE_SIZE;

        if let Some(pg) = sr.pagination {
            page = pg.page;
            page_size = std::cmp::min(pg.page_size, 100) as usize;
        }

        if page == 0 {
            Err(error::Error::Query)
        } else {
            Ok(SearchSpec {
                filter: doc,
                page_size,
                page,
            })
        }
    }
}

impl From<meta_search::ObjectType> for data::ObjectType {
    fn from(v: meta_search::ObjectType) -> Self {
        match v {
            meta_search::ObjectType::Object => data::ObjectType::Object,
            meta_search::ObjectType::ObjectGroup => data::ObjectType::ObjectGroup,
            meta_search::ObjectType::Dataset => data::ObjectType::Dataset,
        }
    }
}

impl From<data::ObjectType> for meta_search::ObjectType {
    fn from(v: data::ObjectType) -> Self {
        match v {
            data::ObjectType::Object => meta_search::ObjectType::Object,
            data::ObjectType::ObjectGroup => meta_search::ObjectType::ObjectGroup,
            data::ObjectType::Dataset => meta_search::ObjectType::Dataset,
        }
    }
}

impl From<data::Label> for meta_search::Label {
    fn from(l: data::Label) -> Self {
        meta_search::Label {
            key: l.key,
            value: l.value,
        }
    }
}

impl From<MetaDataEntry> for meta_search::search_reply::SearchResult {
    fn from(entry: MetaDataEntry) -> Self {
        meta_search::search_reply::SearchResult {
            id: entry.id,
            key: entry.key,
            object_type: Into::<meta_search::ObjectType>::into(entry.object_type) as i32,
            meta_data: entry.metadata.to_string(),
            labels: entry.labels.into_iter().map(Into::into).collect(),
        }
    }
}

#[tonic::async_trait]
impl meta_search::search_server::Search for Searcher {
    async fn search(
        &self,
        request: tonic::Request<crate::meta_search::SearchRequest>,
    ) -> Result<tonic::Response<crate::meta_search::SearchReply>, tonic::Status> {
        debug!("Received search request.");
        let sr = request.into_inner();

        debug!("Parsing search request.");

        let spec: SearchSpec = match sr.try_into() {
            Ok(s) => {
                debug!("Successfuly parsed request: {:?}", &s);
                s
            }
            Err(e) => {
                warn!("Failed to parse search spec: {:?}", &e);
                return Err(tonic::Status::invalid_argument(
                    "Unable to parse your query.",
                ));
            }
        };

        debug!("Executing query.");
        let reply = match self.query_mongo(&spec).await {
            Ok(r) => {
                debug!(
                    "Found {} documents for query: {:?}",
                    r.pagination.as_ref().unwrap().result_count,
                    spec
                );
                r
            }
            Err(e) => {
                error!("Error executing query: {:?}", &e);
                return Err(tonic::Status::internal(
                    "An interal error occured. Please try again later.",
                ));
            }
        };
        Ok(tonic::Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> error::Result<()> {
    let settings = Settings::new().expect("Unable to load configuration.");
    let _logger =
        util::setup_logger("search", &settings.log).expect("Failed to initialize logger.");

    info!("Setting up metadata search.");
    debug!("Configuration: {:?}", &settings);
    info!("Connecting to MongoDB");
    let mongo_client = util::setup_mongo("search", &settings.mongo).await?;

    let collection = mongo_client
        .database(settings.mongo.database.as_str())
        .collection::<data::MetaDataEntry>(settings.mongo.collection.as_str());

    let addr = settings.grpc.bind_address.parse()?;
    let searcher = Searcher { collection };

    info!("Bringing up gRPC server.");
    Server::builder()
        .add_service(meta_search::search_server::SearchServer::new(searcher))
        .serve(addr)
        .await?;

    info!("Shutting down metadata search.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::data::{Label, MetaDataEntry, ObjectType};
    use crate::{meta_search, SearchSpec, Searcher, Settings};
    use config::{Config, File};
    use std::convert::TryInto;
    use uuid::Uuid;

    #[test]
    fn test_query_conversion_all_set() {
        let sr = meta_search::SearchRequest {
            key: Some("key".to_string()),
            object_type: Some(meta_search::ObjectType::Dataset as i32),
            conditions: vec![meta_search::FieldQuery {
                key: "fancy_key".to_owned(),
                query: "\"fancy_value\"".to_owned(),
            }],
            pagination: Some(meta_search::search_request::Pagination {
                page: 2_u32,
                page_size: 50_u32,
            }),
        };

        let spec: SearchSpec = sr.try_into().unwrap();
        let query = spec.filter;

        let expected = mongodb::bson::doc! {
            "object_type" : meta_search::ObjectType::Dataset as i32,
            "key": "key",
            "metadata.fancy_key" : "fancy_value"
        };

        assert_eq!(2_u32, spec.page);
        assert_eq!(50_usize, spec.page_size);
        assert_eq!(expected, query);
    }

    #[test]
    fn test_query_conversion_no_pagination() {
        let sr = meta_search::SearchRequest {
            key: Some("key".to_string()),
            object_type: Some(meta_search::ObjectType::Dataset as i32),
            conditions: vec![meta_search::FieldQuery {
                key: "fancy_key".to_owned(),
                query: "\"fancy_value\"".to_owned(),
            }],
            pagination: None,
        };

        let spec: SearchSpec = sr.try_into().unwrap();
        let query = spec.filter;

        let expected = mongodb::bson::doc! {
            "object_type" : meta_search::ObjectType::Dataset as i32,
            "key": "key",
            "metadata.fancy_key" : "fancy_value"
        };

        assert_eq!(super::DEFAULT_PAGE, spec.page);
        assert_eq!(super::DEFAULT_PAGE_SIZE, spec.page_size);
        assert_eq!(expected, query);
    }

    #[test]
    fn test_query_conversion_no_key() {
        let sr = meta_search::SearchRequest {
            key: None,
            object_type: Some(meta_search::ObjectType::Dataset as i32),
            conditions: vec![meta_search::FieldQuery {
                key: "fancy_key".to_owned(),
                query: "\"fancy_value\"".to_owned(),
            }],
            pagination: None,
        };

        let spec: SearchSpec = sr.try_into().unwrap();
        let query = spec.filter;

        let expected = mongodb::bson::doc! {
            "object_type" : meta_search::ObjectType::Dataset as i32,
            "metadata.fancy_key" : "fancy_value"
        };
        assert_eq!(super::DEFAULT_PAGE, spec.page);
        assert_eq!(super::DEFAULT_PAGE_SIZE, spec.page_size);
        assert_eq!(expected, query);
    }

    #[test]
    fn test_query_conversion_no_object_type() {
        let sr = meta_search::SearchRequest {
            key: Some("key".to_string()),
            object_type: None,
            conditions: vec![meta_search::FieldQuery {
                key: "fancy_key".to_owned(),
                query: "\"fancy_value\"".to_owned(),
            }],
            pagination: None,
        };

        let spec: SearchSpec = sr.try_into().unwrap();
        let query = spec.filter;

        let expected = mongodb::bson::doc! {
            "key": "key",
            "metadata.fancy_key" : "fancy_value"
        };
        assert_eq!(super::DEFAULT_PAGE, spec.page);
        assert_eq!(super::DEFAULT_PAGE_SIZE, spec.page_size);
        assert_eq!(expected, query);
    }
    #[test]
    fn test_query_conversion_no_metadata() {
        let sr = meta_search::SearchRequest {
            key: Some("key".to_string()),
            object_type: Some(meta_search::ObjectType::Dataset as i32),
            conditions: vec![],
            pagination: None,
        };

        let spec: SearchSpec = sr.try_into().unwrap();
        let query = spec.filter;

        let expected = mongodb::bson::doc! {
            "object_type" : meta_search::ObjectType::Dataset as i32,
            "key": "key"
        };
        assert_eq!(super::DEFAULT_PAGE, spec.page);
        assert_eq!(super::DEFAULT_PAGE_SIZE, spec.page_size);
        assert_eq!(expected, query);
    }

    struct CollectionHolder<'a> {
        rt: &'a tokio::runtime::Runtime,
        collection: mongodb::Collection<MetaDataEntry>,
    }

    impl<'a> Drop for CollectionHolder<'a> {
        fn drop(&mut self) {
            self.rt.block_on(self.collection.drop(None)).unwrap();
        }
    }

    async fn setup() -> mongodb::Collection<MetaDataEntry> {
        let mut s = Config::default();
        s.merge(File::with_name("config_test.toml")).unwrap();

        let s: Settings = s.try_into().unwrap();

        let client = crate::common::util::setup_mongo("search_test", &s.mongo)
            .await
            .unwrap();

        let collection_name =
            s.mongo.collection + Uuid::new_v4().to_hyphenated().to_string().as_str();

        let collection = client
            .database(s.mongo.database.as_str())
            .collection::<MetaDataEntry>(collection_name.as_str());

        collection
            .insert_many(
                vec![
                    MetaDataEntry {
                        id: 42,
                        object_type: ObjectType::Dataset,
                        key: "Dataset".to_string(),
                        labels: vec![Label::new("purpose", "test")],
                        metadata: mongodb::bson::doc! { "key" : 350 },
                    },
                    MetaDataEntry {
                        id: 43,
                        object_type: ObjectType::ObjectGroup,
                        key: "ObjectGroup".to_string(),
                        labels: vec![Label::new("purpose", "test")],
                        metadata: mongodb::bson::doc! { "key" : 400 },
                    },
                ],
                None,
            )
            .await
            .unwrap();

        collection
    }

    #[test]
    fn test_empty_query() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::Document::new(),
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(2, r.pagination.unwrap().result_count);
        assert_eq!(2, r.results.len());
        assert_eq!(43, r.results[0].id);
        assert_eq!(42, r.results[1].id);
    }

    #[test]
    fn test_pagination() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 1,
            filter: mongodb::bson::Document::new(),
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(2, r.pagination.unwrap().result_count);
        assert_eq!(1, r.results.len());
        assert_eq!(43, r.results[0].id);

        let spec = SearchSpec {
            page: 2,
            page_size: 1,
            filter: mongodb::bson::Document::new(),
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(2, r.pagination.unwrap().result_count);
        assert_eq!(1, r.results.len());
        assert_eq!(42, r.results[0].id);
    }

    #[test]
    fn test_filter_object_type() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::doc! {
                "object_type": crate::data::ObjectType::Dataset
            },
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(1, r.pagination.unwrap().result_count);
        assert_eq!(1, r.results.len());
        assert_eq!(42, r.results[0].id);
    }

    #[test]
    fn test_filter_id() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::doc! {
                "id" : 43
            },
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(1, r.pagination.unwrap().result_count);
        assert_eq!(1, r.results.len());
        assert_eq!(43, r.results[0].id);
    }

    #[test]
    fn test_filter_id_invalid() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::doc! {
                "id" : 45
            },
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(0, r.pagination.unwrap().result_count);
        assert_eq!(0, r.results.len());
    }

    #[test]
    fn test_filter_key() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::doc! {
                "key" : "Dataset"
            },
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(1, r.pagination.unwrap().result_count);
        assert_eq!(1, r.results.len());
        assert_eq!(42, r.results[0].id);
    }

    #[test]
    fn test_filter_key_invalid() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::doc! {
                "key" : "Something"
            },
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(0, r.pagination.unwrap().result_count);
        assert_eq!(0, r.results.len());
    }

    #[test]
    fn test_meta_data_exact() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::doc! {
                "metadata": {
                    "key": 350
                }
            },
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(1, r.pagination.unwrap().result_count);
        assert_eq!(1, r.results.len());
        assert_eq!(42, r.results[0].id);
    }

    #[test]
    fn test_meta_data_invalid() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::doc! {
                "metadata": {
                    "invalid": "super"
                }
            },
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(0, r.pagination.unwrap().result_count);
        assert_eq!(0, r.results.len());
    }

    #[test]
    fn test_meta_data_expression() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let ch = CollectionHolder {
            collection: rt.block_on(setup()),
            rt: &rt,
        };

        let s = Searcher {
            collection: ch.collection.clone_with_type(),
        };

        let spec = SearchSpec {
            page: 1,
            page_size: 10,
            filter: mongodb::bson::doc! {
                "$or": [
                    {"object_type": {"$regex": "data.*", "$options": "i"}},
                    {"metadata.key": { "$gt": 350 }}
                ]
            },
        };

        let r = rt.block_on(s.query_mongo(&spec)).unwrap();

        assert_eq!(2, r.pagination.unwrap().result_count);
        assert_eq!(2, r.results.len());
        assert_eq!(43, r.results[0].id);
        assert_eq!(42, r.results[1].id);
    }
}