// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{health_checker::HealthChecker, types::ServiceManager, RunLocalTestnet};
use anyhow::Result;
use async_trait::async_trait;
use clap::Parser;
use futures::{Future, FutureExt};
use poem::{
    get, handler,
    http::StatusCode,
    listener::TcpListener,
    middleware::Tracing,
    web::{Data, Json},
    EndpointExt, IntoResponse, Route, Server,
};
use serde::Serialize;
use std::net::{Ipv4Addr, SocketAddrV4};

#[derive(Debug, Clone, Parser)]
pub struct ReadyServerArgs {
    #[clap(long, default_value_t = 8070)]
    pub ready_server_listen_port: u16,
}

#[derive(Debug)]
pub struct ReadyServerManager {
    config: ReadyServerArgs,
    health_checkers: Vec<HealthChecker>,
}

impl ReadyServerManager {
    pub async fn new(
        config: &RunLocalTestnet,
        health_checkers: Vec<HealthChecker>,
    ) -> Result<Self> {
        Ok(ReadyServerManager {
            config: config.ready_server_args.clone(),
            health_checkers,
        })
    }
}

#[async_trait]
impl ServiceManager for ReadyServerManager {
    fn get_name(&self) -> String {
        "Ready Server".to_string()
    }

    fn get_healthchecks(&self) -> Vec<HealthChecker> {
        // We don't health check the service that exposes health checks.
        vec![]
    }

    /// Spawn the node on a thread and then create a future that just waits for it to
    /// exit (which should never happen) forever. This is necessary because there is
    /// no async function we can use to run the node.
    async fn run_service(&self) -> Result<Box<dyn Future<Output = ()>>> {
        let config = self.config.clone();
        let future = run_ready_server(self.health_checkers, config).map(|result| {
            eprintln!("Faucet stopped unexpectedly {:#?}", result);
        });
        Ok(Box::new(future))
    }
}

/// This returns a future that runs a web server that exposes a single unified health
/// checking port. Clients can use this to check if all the services are ready.
pub async fn run_ready_server(
    health_checkers: Vec<HealthChecker>,
    config: ReadyServerArgs,
) -> Result<()> {
    let app = Route::new()
        .at("/", get(root))
        .data(HealthCheckers { health_checkers })
        .with(Tracing);
    Server::new(TcpListener::bind(SocketAddrV4::new(
        Ipv4Addr::new(0, 0, 0, 0),
        config.ready_server_listen_port,
    )))
    .name("ready-server")
    .run(app)
    .await?;
    Err(anyhow::anyhow!("Ready server exited unexpectedly"))
}

#[derive(Clone, Debug)]
struct HealthCheckers {
    pub health_checkers: Vec<HealthChecker>,
}

#[derive(Serialize)]
struct ReadyData {
    pub ready: Vec<HealthChecker>,
    pub not_ready: Vec<HealthChecker>,
}

#[handler]
async fn root(health_checkers: Data<&HealthCheckers>) -> impl IntoResponse {
    let mut ready = vec![];
    let mut not_ready = vec![];
    for health_checker in &health_checkers.health_checkers {
        match health_checker.check().await {
            Ok(()) => ready.push(health_checker.clone()),
            Err(_) => {
                not_ready.push(health_checker.clone());
            },
        }
    }
    let status_code = if not_ready.is_empty() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Json(ReadyData { ready, not_ready }).with_status(status_code)
}
