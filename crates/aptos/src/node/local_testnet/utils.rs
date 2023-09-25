// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Result};
use clap::Parser;
use reqwest::Url;
use std::net::SocketAddr;

pub fn socket_addr_to_url(socket_addr: &SocketAddr, scheme: &str) -> Result<Url> {
    let host = match socket_addr {
        SocketAddr::V4(v4) => format!("{}", v4.ip()),
        SocketAddr::V6(v6) => format!("[{}]", v6.ip()),
    };
    let full_url = format!("{}://{}:{}", scheme, host, socket_addr.port());
    Ok(Url::parse(&full_url)?)
}

#[derive(Clone, Parser)]
pub struct HostPostgresArgs {
    #[clap(long, default_value = "127.0.0.1")]
    pub postgres_host: String,

    #[clap(long, default_value_t = 5432)]
    pub postgres_port: u16,

    #[clap(long, default_value = "postgres")]
    pub postgres_user: String,

    #[clap(long)]
    pub postgres_password: Option<String>,

    #[clap(long, default_value = "local_testnet")]
    pub postgres_database: String,
}

impl HostPostgresArgs {
    /// Get the connection string for the postgres database. If `database` is specified
    /// we will use that rather than `postgres_database`.
    pub fn get_connection_string(&self, database: Option<&str>) -> String {
        let password = match &self.postgres_password {
            Some(password) => format!(":{}", password),
            None => "".to_string(),
        };
        let database = match database {
            Some(database) => database,
            None => &self.postgres_database,
        };
        format!(
            "postgres://{}{}@{}:{}/{}",
            self.postgres_user, password, self.postgres_host, self.postgres_port, database,
        )
    }
}

#[derive(Clone, Parser)]
pub struct IndexerApiConfig {
    #[clap(long, default_value_t = 8090)]
    pub indexer_api_port: u16,
}

/// This submits a POST request to apply metadata to a Hasura API.
pub async fn post_metadata(metadata_content: &str, port: u16) -> Result<()> {
    let url = format!("http://127.0.0.1:{}/v1/metadata", port);
    let client = reqwest::Client::new();

    // Parse the metadata content as JSON.
    let metadata_json: serde_json::Value = serde_json::from_str(metadata_content)?;

    // Construct the payload.
    let mut payload = serde_json::Map::new();
    payload.insert(
        "type".to_string(),
        serde_json::Value::String("replace_metadata".to_string()),
    );
    payload.insert("args".to_string(), metadata_json);

    // Send the POST request.
    let response = client.post(url).json(&payload).send().await?;

    // Check that `is_consistent` is true in the response.
    let json = response.json().await?;
    check_is_consistent(&json)?;

    Ok(())
}

fn check_is_consistent(json: &serde_json::Value) -> Result<()> {
    if let Some(obj) = json.as_object() {
        if let Some(is_consistent_val) = obj.get("is_consistent") {
            if is_consistent_val.as_bool() == Some(true) {
                return Ok(());
            }
        }
    }

    bail!(
        "Something went wrong applying the Hasura metadata. Response: {:#?}",
        json
    );
}
