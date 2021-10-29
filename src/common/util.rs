use flexi_logger::{LogSpecBuilder, Logger, LoggerHandle};
use log::LevelFilter;
use mongodb::bson::doc;
use mongodb::options::{ClientOptions, ServerAddress};
use std::str::FromStr;

use crate::common::settings::{LogSettings, MongoSettings};

use super::error::Result;

pub fn setup_logger(module_name: &str, settings: &LogSettings) -> Result<LoggerHandle> {
    let mut builder = LogSpecBuilder::new();
    let level = LevelFilter::from_str(settings.level.as_str())?;
    builder
        .default(LevelFilter::Info)
        .module(module_name, level);
    Ok(Logger::with(builder.build())
        .format_for_stderr(flexi_logger::colored_detailed_format)
        .start()?)
}

pub async fn setup_mongo(module_name: &str, settings: &MongoSettings) -> Result<mongodb::Client> {
    let mut addresses: Vec<ServerAddress> = Vec::with_capacity(settings.servers.len());

    for addr_str in settings.servers.iter() {
        addresses.push(ServerAddress::parse(addr_str.as_str())?);
    }

    let client_options = ClientOptions::builder()
        .hosts(addresses)
        .app_name(Some(module_name.to_string()));

    let client = mongodb::Client::with_options(client_options.build())?;

    // Execute ping
    client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await?;

    Ok(client)
}
