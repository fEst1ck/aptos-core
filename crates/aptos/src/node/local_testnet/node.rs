// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{health_checker::HealthChecker, types::ServiceManager, RunLocalTestnet};
use crate::node::local_testnet::utils::socket_addr_to_url;
use anyhow::{Context, Result};
use aptos_config::config::{NodeConfig, DEFAULT_GRPC_STREAM_PORT};
use aptos_node::create_single_node_test_config;
use async_trait::async_trait;
use clap::Parser;
use futures::{Future, FutureExt};
use rand::{rngs::StdRng, SeedableRng};
use std::{path::PathBuf, thread, time::Duration};

/// TODO:
#[derive(Debug, Parser)]
pub struct NodeArgs {
    /// An overridable config template for the test node
    ///
    /// If provided, the config will be used, and any needed configuration for the local testnet
    /// will override the config's values
    #[clap(long, value_parser)]
    pub config_path: Option<PathBuf>,

    /// Path to node configuration file override for local test mode.
    ///
    /// If provided, the default node config will be overridden by the config in the given file.
    /// Cannot be used with --config-path
    #[clap(long, value_parser, conflicts_with("config_path"))]
    pub test_config_override: Option<PathBuf>,

    /// Random seed for key generation in test mode
    ///
    /// This allows you to have deterministic keys for testing
    #[clap(long, value_parser = aptos_node::load_seed)]
    pub seed: Option<[u8; 32]>,

    /// Do not run a transaction stream service alongside the node.
    ///
    /// Note: In reality this is not the same as running a Transaction Stream Service,
    /// it is just using the stream from the node, but in practice this distinction
    /// shouldn't matter.
    #[clap(long)]
    no_txn_stream: bool,

    /// The port at which to expose the grpc transaction stream.
    #[clap(long, default_value_t = DEFAULT_GRPC_STREAM_PORT)]
    txn_stream_port: u16,
}

#[derive(Debug)]
pub struct NodeManager {
    config: NodeConfig,
    // These are only required because of deficiencies in the "API" for starting a
    // node, we can fix this one day.
    test_dir: PathBuf,
    seed: Option<[u8; 32]>,
}

impl NodeManager {
    pub async fn new(config: &RunLocalTestnet, test_dir: PathBuf) -> Result<Self> {
        let rng = config
            .node_args
            .seed
            .map(StdRng::from_seed)
            .unwrap_or_else(StdRng::from_entropy);

        // TODO: Something seems wrong. If test_dir already exists (and at this point
        // all the force restart stuff has been resolved) should I even be doing this?
        let mut node_config = create_single_node_test_config(
            &config.node_args.config_path,
            &config.node_args.test_config_override,
            &test_dir,
            false,
            false,
            aptos_cached_packages::head_release_bundle(),
            rng,
        )
        .context("Failed to create config for node")?;

        eprintln!();

        // Enable the grpc stream on the node if we will run a txn stream service.
        let run_txn_stream = !config.node_args.no_txn_stream;
        node_config.indexer_grpc.enabled = run_txn_stream;
        node_config.indexer_grpc.use_data_service_interface = run_txn_stream;
        node_config
            .indexer_grpc
            .address
            .set_port(config.node_args.txn_stream_port);

        // So long as the indexer relies on storage indexing tables, this must be set
        // for the indexer GRPC stream on the node to work.
        node_config.storage.enable_indexer = run_txn_stream;

        Ok(NodeManager {
            config: node_config,
            test_dir,
            seed: config.node_args.seed,
        })
    }
}

#[async_trait]
impl ServiceManager for NodeManager {
    fn get_name(&self) -> String {
        "Node API".to_string()
    }

    fn get_healthchecks(&self) -> Vec<HealthChecker> {
        let node_api_url = socket_addr_to_url(&self.config.api.address, "http").unwrap();
        let mut checkers = vec![HealthChecker::NodeApi(node_api_url)];
        if self.config.indexer_grpc.enabled {
            let data_service_url =
                socket_addr_to_url(&self.config.indexer_grpc.address, "http").unwrap();
            checkers.push(HealthChecker::DataServiceGrpc(data_service_url));
        }
        checkers
    }

    /// Spawn the node on a thread and then create a future that just waits for it to
    /// exit (which should never happen) forever. This is necessary because there is
    /// no async function we can use to run the node.
    async fn run_service(&self) -> Result<Box<dyn Future<Output = ()>>> {
        let rng = self
            .seed
            .map(StdRng::from_seed)
            .unwrap_or_else(StdRng::from_entropy);

        let node_thread_handle = thread::spawn(move || {
            let result = aptos_node::setup_test_environment_and_start_node(
                None,
                None,
                Some(self.config.clone()),
                Some(self.test_dir),
                false,
                false,
                aptos_cached_packages::head_release_bundle(),
                rng,
            );
            eprintln!("Node stopped unexpectedly {:#?}", result);
        });

        // This just waits for the node thread forever.
        let future = async move {
            loop {
                if node_thread_handle.is_finished() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        };

        Ok(Box::new(future))
    }
}
