// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{health_checker::HealthChecker, types::ServiceManager, RunLocalTestnet, utils::HostPostgresArgs};
use anyhow::Result;
use aptos_faucet_core::server::{FunderKeyEnum, RunConfig};
use async_trait::async_trait;
use clap::Parser;
use futures::{Future, FutureExt};
use reqwest::Url;
use std::path::PathBuf;
use processor::{
    processors::{token_processor::TokenProcessorConfig, ProcessorConfig, ProcessorNames},
    utils::database::{new_db_pool, recreate_database},
    IndexerGrpcProcessorConfig,
};
use server_framework::{run_server_with_config, GenericConfig};

// Hmm this processors flag is weird. For now I'll just jam them all together.

/// TODO:
#[derive(Debug, Parser)]
pub struct ProcessorArgs {
    /// The value of this flag determines which processors we will run if
    /// --with-indexer-api is set. Note that some processors are not supported in the
    /// local testnet (e.g. ANS). If you try to set those, an error will be thrown
    /// immediately.
    #[clap(
        long,
        value_enum,
        default_values_t = vec![
            ProcessorNames::CoinProcessor,
            ProcessorNames::DefaultProcessor,
            ProcessorNames::EventsProcessor,
            ProcessorNames::FungibleAssetProcessor,
            ProcessorNames::StakeProcessor,
            ProcessorNames::TokenProcessor,
            ProcessorNames::TokenV2Processor,
            ProcessorNames::UserTransactionProcessor,
        ],
        requires = "with_indexer_api"
    )]
    processors: Vec<ProcessorNames>,
}

/// TODO: Explain that this does many at once.
#[derive(Debug)]
pub struct ProcessorManager {
    configs: Vec<GenericConfig<IndexerGrpcProcessorConfig>>,
    postgres_connection_string: String,
}

impl ProcessorManager {
    pub async fn new(
        config: &RunLocalTestnet,
        test_dir: PathBuf,
        node_api_url: Url,
    ) -> Result<Self> {
        Ok(Self {
            config: RunConfig::build_for_cli(
                node_api_url.clone(),
                config.faucet_args.faucet_port,
                FunderKeyEnum::KeyFile(test_dir.join("mint.key")),
                config.faucet_args.do_not_delegate,
                None,
            ),
        })
    }
}

#[async_trait]
impl ServiceManager for ProcessorManager {
    fn get_name(&self) -> String {
        "Faucet".to_string()
    }

    fn get_healthchecks(&self) -> Vec<HealthChecker> {

        let postgres_connection_string = match self.use_host_postgres {
            true => self.postgres_args.get_connection_string(None),
            false => {
                // TODO: Create config to run postgres with Docker. Try to use 5432 but
                // pick another open port otherwise.
                unimplemented!("You must set --use-host-postgres for now");
            },
        };
        let mut health_check_port = 43234;
        for processor_name in &self.processors {
            let processor_config = match processor_name {
                ProcessorNames::AnsProcessor => {
                    bail!("ANS processor is not supported in the local testnet")
                },
                ProcessorNames::CoinProcessor => ProcessorConfig::CoinProcessor,
                ProcessorNames::DefaultProcessor => ProcessorConfig::DefaultProcessor,
                ProcessorNames::EventsProcessor => ProcessorConfig::EventsProcessor,
                ProcessorNames::FungibleAssetProcessor => {
                    ProcessorConfig::FungibleAssetProcessor
                },
                ProcessorNames::NFTMetadataProcessor => {
                    bail!("NFT Metadata processor is not supported in the local testnet")
                },
                ProcessorNames::StakeProcessor => ProcessorConfig::StakeProcessor,
                ProcessorNames::TokenProcessor => {
                    ProcessorConfig::TokenProcessor(TokenProcessorConfig {
                        // TODO: What should this be / what does it mean?
                        nft_points_contract: None,
                    })
                },
                ProcessorNames::TokenV2Processor => ProcessorConfig::TokenV2Processor,
                ProcessorNames::UserTransactionProcessor => {
                    ProcessorConfig::UserTransactionProcessor
                },
            };
            let server_config = IndexerGrpcProcessorConfig {
                processor_config,
                postgres_connection_string: postgres_connection_string.clone(),
                indexer_grpc_data_service_address: data_service_url.clone(),
                auth_token: "notused".to_string(),
                grpc_http2_config: Default::default(),
                starting_version: None,
                ending_version: None,
                number_concurrent_processing_tasks: None,
            };
            let config = GenericConfig {
                server_config,
                health_check_port,
            };
            processor_configs.push(config);
            health_check_port += 1;
        }

        for config in self.configs {
            match processor {
                ProcessorConfig::TokenProcessor(config) => {
                    let db_pool = new_db_pool(&config.database_url)?;
                    recreate_database(&db_pool)?;
                }
                _ => {}
            }
        }
        vec![HealthChecker::Http(
            Url::parse(&format!(
                "http://127.0.0.1:{}",
                self.config.server_config.listen_port
            ))
            .unwrap(),
            "Faucet".to_string(),
        )]


        health_checks.push(HealthChecker::Http(
            Url::parse(&format!(
                "http://127.0.0.1:{}/",
                self.indexer_api_args.indexer_api_port
            ))
            .unwrap(),
            "Indexer API".to_string(),
        ));
    }

    async fn run_service(&self) -> Result<Box<dyn Future<Output = ()>>> {
        let future = self.config.run().map(|result| {
            eprintln!("Faucet stopped unexpectedly {:#?}", result);
        });
        Ok(Box::new(future))
    }
}
