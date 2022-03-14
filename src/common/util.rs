use flexi_logger::{LogSpecBuilder, Logger, LoggerHandle};
use log::LevelFilter;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
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

pub async fn setup_mongo(settings: &MongoSettings) -> Result<mongodb::Client> {
    let client_options = ClientOptions::parse(&settings.connection_string).await?;
    let client = mongodb::Client::with_options(client_options)?;

    // Execute ping
    client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await?;

    Ok(client)
}
