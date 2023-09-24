// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use reqwest::Url;
use std::net::SocketAddr;

pub fn socket_addr_to_url(socket_addr: &SocketAddr, scheme: &str) -> anyhow::Result<Url> {
    let host = match socket_addr {
        SocketAddr::V4(v4) => format!("{}", v4.ip()),
        SocketAddr::V6(v6) => format!("[{}]", v6.ip()),
    };
    let full_url = format!("{}://{}:{}", scheme, host, socket_addr.port());
    Ok(Url::parse(&full_url)?)
}

#[derive(Parser)]
pub struct HostPostgresArgs {
    #[clap(long, default_value = "127.0.0.1")]
    pub postgres_host: String,

    #[clap(long, default_value = "5432")]
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
