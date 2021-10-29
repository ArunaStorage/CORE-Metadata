use common::settings::{LogSettings, MongoSettings};
use common::{data, error, util};
use data::MetaDataEntry;
use error::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use log::{debug, error, info, warn};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Message};

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};

mod common;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct KafkaSettings {
    pub topic: String,
    pub group: String,
    brokers: Vec<String>,
}

impl KafkaSettings {
    pub fn broker_string(&self) -> String {
        self.brokers.join(",")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Settings {
    pub mongo: MongoSettings,
    pub kafka: KafkaSettings,
    pub log: LogSettings,
    pub consumer_count: u32,
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

async fn run_kafka_consumers(
    settings: &Settings,
    consumer: Box<dyn MessageConsumer>,
) -> Result<()> {
    info!(
        "Spawning {} Kafka-Consumers. Settings: {:?}",
        settings.consumer_count, &settings.kafka
    );

    let mut consumers: Vec<StreamConsumer> = Vec::with_capacity(settings.consumer_count as usize);

    for _ in 0..settings.consumer_count {
        let c = create_kafka_consumer(&settings.kafka).await?;
        c.subscribe(&[&settings.kafka.topic])?;
        consumers.push(c);
    }

    info!("Starting consumers.");

    consumers
        .into_iter()
        .map(|c| tokio::spawn(run_kafka_consumer(c, consumer.box_clone())))
        .collect::<FuturesUnordered<_>>()
        .for_each(|_| async {})
        .await;
    Ok(())
}

async fn create_kafka_consumer(settings: &KafkaSettings) -> Result<StreamConsumer> {
    info!("Creating Kafka-Consumer.");

    let consumer = ClientConfig::new()
        .set("group.id", &settings.group.clone())
        .set("bootstrap.servers", settings.broker_string())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()?;
    Ok(consumer)
}

async fn run_kafka_consumer(
    consumer: StreamConsumer,
    message_consumer: Box<dyn MessageConsumer>,
) -> Result<()> {
    debug!("Entering receive loop.");
    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        match result {
            Ok(msg) => match message_consumer.consume(&msg).await {
                Ok(_) => {
                    debug!("Successfully processed message. Commiting.");
                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                        error!("Failed to commit consumption of message: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Error processing message: {:?}", e)
                }
            },
            Err(e) => {
                error!("Failed to fetch message from broker: {:?}", e);
            }
        }
    }
    info!("Stream processing terminated");
    Ok(())
}

#[async_trait::async_trait]
pub trait MessageConsumer: Send + Sync {
    async fn consume(&self, msg: &BorrowedMessage) -> Result<()>;
    fn box_clone(&self) -> Box<dyn MessageConsumer>;
}

impl Clone for Box<dyn MessageConsumer> {
    fn clone(&self) -> Box<dyn MessageConsumer> {
        self.box_clone()
    }
}

#[derive(Clone)]
struct MongoInserter {
    mongo_client: mongodb::Client,
    settings: MongoSettings,
}

#[async_trait::async_trait]
impl MessageConsumer for MongoInserter {
    async fn consume(&self, msg: &BorrowedMessage) -> Result<()> {
        if let Some(pl) = msg.payload() {
            debug!("Received message ({} bytes)", pl.len());
            let val: MetaDataEntry = serde_json::from_slice(pl)?;

            let ir = self
                .mongo_client
                .database(self.settings.database.as_str())
                .collection::<MetaDataEntry>(self.settings.collection.as_str())
                .insert_one(&val, None)
                .await?;

            debug!(
                "Successfuly inserted Metadata (id={}): {:?}",
                &ir.inserted_id, &val
            );
        } else {
            warn!("Received message with empty payload.")
        }
        Ok(())
    }

    fn box_clone(&self) -> Box<dyn MessageConsumer> {
        Box::new(self.clone())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::new().expect("Unable to load configuration.");
    let _logger =
        util::setup_logger("importer", &settings.log).expect("Failed to initialize logger.");

    info!("Setting up metadata loader.");
    debug!("Configuration: {:?}", &settings);
    info!("Connecting to MongoDB");
    let mongo_client = util::setup_mongo("importer", &settings.mongo).await?;

    let consumer = Box::new(MongoInserter {
        mongo_client,
        settings: settings.mongo.clone(),
    });

    info!("Starting Kafka consumers.");
    run_kafka_consumers(&settings, consumer).await?;

    info!("Shutting down metadata loader.");
    Ok(())
}
