use common::settings::{LogSettings, MongoSettings};
use common::{data, error, util};
use data::MetaDataEntry;
use error::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{debug, error, info};
use std::str::FromStr;

use config::{Config, ConfigError, Environment, File};
use mongodb::Client;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::notification::services::v1::{CreateEventStreamingGroupRequest, NotficationStreamAck, NotificationStreamGroupRequest, NotificationStreamInit, NotificationStreamResponse};
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::notification::services::v1::event_notification_message::UpdateType;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::notification::services::v1::notification_stream_group_request::StreamAction;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::notification::services::v1::update_notification_service_client::UpdateNotificationServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::models::v1::{Dataset, DatasetVersion, ObjectGroup, Project, Resource};
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::dataset_objects_service_client::DatasetObjectsServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::dataset_service_client::DatasetServiceClient;
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::{GetDatasetRequest, GetDatasetVersionRequest, GetObjectGroupRequest, GetProjectRequest};
use scienceobjectsdb_rust_api::sciobjectsdb::sciobjsdb::api::storage::services::v1::project_service_client::ProjectServiceClient;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::{Request, Status};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use crate::data::{HasMetadata, ObjectType};
use crate::error::Error;

mod common;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct NotificationSettings {
    pub group: String,
    pub endpoint: String,
    pub project_id: String,
    pub api_token: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Settings {
    pub mongo: MongoSettings,
    pub notifications: NotificationSettings,
    pub log: LogSettings,
}

impl Settings {
    pub fn new() -> std::result::Result<Self, ConfigError> {
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

/// The interceptor appends the `API_TOKEN` to each request
#[derive(Clone, Debug)]
struct APITokenInterceptor {
    key: AsciiMetadataKey,
    token: AsciiMetadataValue,
}

impl APITokenInterceptor {
    fn new(token: &str) -> Result<APITokenInterceptor> {
        let key = AsciiMetadataKey::from_static("api_token");
        let token = AsciiMetadataValue::from_str(token)?;
        Ok(APITokenInterceptor { key, token })
    }
}

impl Interceptor for APITokenInterceptor {
    // Append the API token to the given request
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        request
            .metadata_mut()
            .append(self.key.clone(), self.token.clone());
        Ok(request)
    }
}

#[derive(Clone)]
struct GrpcClient {
    project_client: ProjectServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    dataset_client: DatasetServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    object_group_client:
        DatasetObjectsServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    // object_client: ObjectLoadServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
    notifications_client:
        UpdateNotificationServiceClient<InterceptedService<Channel, APITokenInterceptor>>,
}

impl GrpcClient {
    async fn new(settings: &Settings) -> Result<GrpcClient> {
        let channel = Endpoint::from_str(settings.notifications.endpoint.as_str())?
            .connect()
            .await?;
        // Create an interceptor instance
        let interceptor = APITokenInterceptor::new(settings.notifications.api_token.as_str())?;

        Ok(GrpcClient {
            project_client: ProjectServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            dataset_client: DatasetServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            object_group_client: DatasetObjectsServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            // object_client: ObjectLoadServiceClient::with_interceptor(
            //     channel.clone(),
            //     interceptor.clone(),
            // ),
            notifications_client: UpdateNotificationServiceClient::with_interceptor(
                channel,
                interceptor,
            ),
        })
    }

    async fn get_project(&self, id: String) -> Result<Project> {
        let resp = self
            .project_client
            .clone()
            .get_project(GetProjectRequest { id: id.clone() })
            .await?
            .into_inner();

        resp.project.ok_or(Error::RsourceNotFound {
            id,
            resource_type: ObjectType::Project,
        })
    }

    async fn get_dataset(&self, id: String) -> Result<Dataset> {
        let resp = self
            .dataset_client
            .clone()
            .get_dataset(GetDatasetRequest { id: id.clone() })
            .await?
            .into_inner();

        resp.dataset.ok_or(Error::RsourceNotFound {
            id,
            resource_type: ObjectType::Dataset,
        })
    }

    async fn get_dataset_version(&self, id: String) -> Result<DatasetVersion> {
        let resp = self
            .dataset_client
            .clone()
            .get_dataset_version(GetDatasetVersionRequest { id: id.clone() })
            .await?
            .into_inner();

        resp.dataset_version.ok_or(Error::RsourceNotFound {
            id,
            resource_type: ObjectType::DatasetVersion,
        })
    }

    async fn get_object_group(&self, id: String) -> Result<ObjectGroup> {
        let resp = self
            .object_group_client
            .clone()
            .get_object_group(GetObjectGroupRequest { id: id.clone() })
            .await?
            .into_inner();

        resp.object_group.ok_or(Error::RsourceNotFound {
            id,
            resource_type: ObjectType::ObjectGroup,
        })
    }
}

async fn notifications_loop(
    settings: &Settings,
    mut grpc: GrpcClient,
    consumer: Box<dyn MessageConsumer>,
) -> Result<()> {
    // We need a channel to send the initial request and acknowledgements to the server
    let (tx, rx) = tokio::sync::mpsc::channel(8);

    let group_id = if settings.notifications.group.eq_ignore_ascii_case("CREATE") {
        info!("Creating new stream group.");
        grpc.notifications_client
            .create_event_streaming_group(CreateEventStreamingGroupRequest {
                resource: Resource::Project as i32,
                resource_id: settings.notifications.project_id.clone(),
                include_subresource: true,
                stream_type: Default::default(),
                stream_group_id: Default::default(),
            })
            .await?
            .into_inner()
            .stream_group_id
    } else {
        settings.notifications.group.clone()
    };

    info!("Subscribing to stream for group {}", &group_id);

    // Send the initial request
    tx.send(NotificationStreamGroupRequest {
        // Do not close the notification stream
        close: false,
        // Submit the group id
        stream_action: Some(StreamAction::Init(NotificationStreamInit {
            stream_group_id: group_id,
        })),
    })
    .await?;

    // Initialize the server connection
    let mut notification_stream = grpc
        .notifications_client
        .notification_stream_group(ReceiverStream::new(rx))
        .await?
        .into_inner();

    // Wait and process the notifications until the stream ends or an error is received
    while let Some(Ok(batch)) = notification_stream.next().await {
        debug!(
            "Processing chunk of {} notifications.",
            batch.notification.len()
        );

        // Update the database
        match consumer.process_messages(&batch.notification).await {
            // OK -> Acknowledge the messages
            Ok(_) => {
                debug!("Acknowledging message batch: {}", &batch.ack_chunk_id);
                tx.send(NotificationStreamGroupRequest {
                    // Do not close the notification stream
                    close: false,
                    // Submit the group id
                    stream_action: Some(StreamAction::Ack(NotficationStreamAck {
                        ack_chunk_id: vec![batch.ack_chunk_id],
                    })),
                })
                .await?
            }
            Err(e) => {
                error!(
                    "Not acknowledging chunk {} due to error on metadata update: {:?}",
                    &batch.ack_chunk_id, e
                );
            }
        }
    }
    Ok(())
}

#[async_trait::async_trait]
pub trait MessageConsumer: Send + Sync {
    async fn process_messages(&self, msgs: &Vec<NotificationStreamResponse>) -> Result<()>;
    fn box_clone(&self) -> Box<dyn MessageConsumer>;
}

impl Clone for Box<dyn MessageConsumer> {
    fn clone(&self) -> Box<dyn MessageConsumer> {
        self.box_clone()
    }
}

#[derive(Clone)]
struct MongoInserter {
    grpc: GrpcClient,
    mongo_client: mongodb::Client,
    settings: MongoSettings,
}

impl MongoInserter {
    async fn handle_message<T: HasMetadata>(
        &self,
        client: Client,
        update_type: i32,
        resource: T,
    ) -> Result<()> {
        let collection = client
            .database(self.settings.database.as_str())
            .collection::<MetaDataEntry>(self.settings.collection.as_str());

        // delete old entries
        let del_result = collection
            .delete_many(
                serde_json::from_value(serde_json::json!({ "id": resource.resource_id() }))
                    .expect("Is valid json!"),
                None,
            )
            .await?;
        debug!(
            "Removed {} metadata entries for resource {}.",
            del_result.deleted_count,
            resource.resource_id()
        );

        // Re-insert on update or new
        if update_type != UpdateType::Deleted as i32 {
            let metadatas = resource.meta_data();
            if !metadatas.is_empty() {
                let insert_result = collection.insert_many(metadatas, None).await?;
                debug!(
                    "Inserted {} metadata entries for resource {}.",
                    insert_result.inserted_ids.len(),
                    resource.resource_id()
                );
            }
        }
        Ok(())
    }

    async fn consume_message(
        &self,
        client: Client,
        msg: &NotificationStreamResponse,
    ) -> Result<()> {
        let message = msg
            .message
            .as_ref()
            .expect("Expecting a message in a notification.");

        match message.resource {
            x if x == Resource::Project as i32 => {
                let project = self.grpc.get_project(message.resource_id.clone()).await?;
                self.handle_message(client, message.updated_type, project)
                    .await?;
            }
            x if x == Resource::Dataset as i32 => {
                let dataset = self.grpc.get_dataset(message.resource_id.clone()).await?;
                self.handle_message(client, message.updated_type, dataset)
                    .await?;
            }
            x if x == Resource::DatasetVersion as i32 => {
                let dataset_version = self
                    .grpc
                    .get_dataset_version(message.resource_id.clone())
                    .await?;
                self.handle_message(client, message.updated_type, dataset_version)
                    .await?;
            }
            x if x == Resource::ObjectGroup as i32 => {
                let object_group = self
                    .grpc
                    .get_object_group(message.resource_id.clone())
                    .await?;
                self.handle_message(client, message.updated_type, object_group)
                    .await?;
            }
            _ => {
                return Err(Error::UnknownResourceType {
                    resource_type: message.resource,
                })
            }
        };
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageConsumer for MongoInserter {
    async fn process_messages(&self, msgs: &Vec<NotificationStreamResponse>) -> Result<()> {
        if msgs.is_empty() {
            return Ok(());
        }

        let mut has_errors = false;

        // Begin a transaction
        let mut session = self.mongo_client.start_session(None).await?;

        session.start_transaction(None).await?;

        // Loop the notification of the current batch
        let mut futures = msgs
            .iter()
            .map(|n| self.consume_message(session.client(), n))
            .collect::<FuturesUnordered<_>>();

        while let Some(result) = futures.next().await {
            if let Err(e) = result {
                error!("Error processing message: {:?}", e);
                has_errors = true;
            }
        }
        if has_errors {
            // Errors occured abort transaction
            session.abort_transaction().await?;
            Err(Error::MessageProcessing)
        } else {
            // All good, commit
            session.commit_transaction().await?;
            Ok(())
        }
    }

    fn box_clone(&self) -> Box<dyn MessageConsumer> {
        Box::new(self.clone())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::new().expect("Unable to load configuration.");
    let _logger =
        util::setup_logger("loader", &settings.log).expect("Failed to initialize logger.");

    info!("Setting up metadata loader.");
    debug!("Configuration: {:?}", &settings);
    info!("Connecting to MongoDB");
    let mongo_client = util::setup_mongo(&settings.mongo).await?;

    info!(
        "Setting up gRPC connection to {}",
        &settings.notifications.endpoint
    );

    let grpc = GrpcClient::new(&settings).await?;

    let consumer = Box::new(MongoInserter {
        mongo_client,
        grpc: grpc.clone(),
        settings: settings.mongo.clone(),
    });

    info!("Starting notifications loop.");
    notifications_loop(&settings, grpc, consumer).await?;

    info!("Shutting down metadata loader.");
    Ok(())
}
