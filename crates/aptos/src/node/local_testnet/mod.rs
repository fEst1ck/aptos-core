// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

mod faucet;
mod health_checker;
mod logging;
mod node;
mod ready_server;
mod processors;
mod types;
mod utils;

use self::{
    faucet::FaucetArgs,
    health_checker::HealthChecker,
    logging::ThreadNameMakeWriter,
    node::NodeArgs,
    ready_server::{run_ready_server, ReadyServerArgs},
    utils::{socket_addr_to_url, HostPostgresArgs, IndexerApiConfig},
};
use crate::{
    common::{
        types::{CliCommand, CliError, CliTypedResult, ConfigSearchMode, PromptOptions},
        utils::prompt_yes_with_override,
    },
    config::GlobalConfig,
    node::local_testnet::utils::post_metadata,
};
use anyhow::{bail, Context};
use aptos_config::config::{NodeConfig, DEFAULT_GRPC_STREAM_PORT};
use aptos_faucet_core::server::FunderKeyEnum;
use aptos_indexer_grpc_server_framework::setup_logging;
use aptos_node::create_single_node_test_config;
use async_trait::async_trait;
use clap::Parser;
use futures::{Future, FutureExt};
use processor::{
    processors::{token_processor::TokenProcessorConfig, ProcessorConfig, ProcessorNames},
    utils::database::{new_db_pool, recreate_database},
    IndexerGrpcProcessorConfig,
};
use rand::{rngs::StdRng, SeedableRng};
use reqwest::Url;
use server_framework::{run_server_with_config, GenericConfig};
use std::{
    fs::{create_dir_all, remove_dir_all},
    path::PathBuf,
    pin::Pin,
    process::Stdio,
    thread,
    time::Duration,
};
use tokio::{process::Command, task::JoinHandle};
use tracing::info;
use tracing_subscriber::fmt::MakeWriter;

const TESTNET_FOLDER: &str = "testnet";

const INDEXER_API_CONTAINER_NAME: &str = "indexer-api";
const HASURA_METADATA: &str = include_str!("hasura_metadata.json");

/// Run a local testnet
///
/// This local testnet will run it's own genesis and run as a single node network
/// locally. A faucet and grpc transaction stream will run alongside the node unless
/// you specify otherwise with --no-faucet and --no-txn-stream respectively.
#[derive(Parser)]
pub struct RunLocalTestnet {
    /// The directory to save all files for the node
    ///
    /// Defaults to .aptos/testnet
    #[clap(long, value_parser)]
    test_dir: Option<PathBuf>,

    /// Clean the state and start with a new chain at genesis
    ///
    /// This will wipe the aptosdb in `test-dir` to remove any incompatible changes, and start
    /// the chain fresh.  Note, that you will need to publish the module again and distribute funds
    /// from the faucet accordingly
    #[clap(long)]
    force_restart: bool,

    #[clap(flatten)]
    node_args: NodeArgs,

    #[clap(flatten)]
    faucet_args: FaucetArgs,

    /// If set, we will run a postgres DB using Docker (unless
    /// --use-host-postgres is set), run the standard set of indexer processors (see
    /// --processors) and configure them to write to this DB, and run an API that lets
    /// you access the data they write to storage. This is opt in because it requires
    /// Docker to be installed in the host system.
    #[clap(long, conflicts_with = "no_txn_stream")]
    with_indexer_api: bool,

    /// If set, connect to the postgres instance specified in `postgres_args` (e.g.
    /// --postgres-host, --postgres-user, etc) rather than running a new one with
    /// Docker. This can be used to connect to an existing postgres instance running
    /// on the host system. Do not include the database. WARNING: This tool will drop
    /// any existing database it finds.
    #[clap(long, requires = "with_indexer_api")]
    use_host_postgres: bool,

    #[clap(flatten)]
    postgres_args: HostPostgresArgs,

    #[clap(flatten)]
    indexer_api_args: IndexerApiConfig,

    #[clap(flatten)]
    ready_server_args: ReadyServerArgs,

    #[clap(flatten)]
    prompt_options: PromptOptions,
}

#[derive(Debug)]
struct AllConfigs {
    ready_server_config: ReadyServerArgs,
    node_config: NodeConfig,
    faucet_config: Option<FaucetArgs>,
    processor_configs: Vec<GenericConfig<IndexerGrpcProcessorConfig>>,
}

impl AllConfigs {
    pub fn get_node_api_url(&self) -> Url {
        socket_addr_to_url(&self.node_config.api.address, "http").unwrap()
    }

    pub fn get_data_service_url(&self) -> Url {
        socket_addr_to_url(&self.node_config.indexer_grpc.address, "http").unwrap()
    }
}

impl RunLocalTestnet {
    pub fn get_postgres_connection_string(&self) -> String {
        match self.use_host_postgres {
            true => self.postgres_args.get_connection_string(None),
            false => {
                // TODO: Create config to run postgres with Docker. Try to use 5432 but
                // pick another open port otherwise.
                unimplemented!("You must set --use-host-postgres for now");
            }
        }
    }

    /// This function builds all the configs we need to run each of the requested
    /// services. We separate creating configs and spawning services to keep the
    /// code clean. This could also allow us to one day have two phases for starting
    /// a local testnet, in which you can alter the configs on disk between each phase.
    fn build_configs(&self, test_dir: PathBuf) -> anyhow::Result<AllConfigs> {
        let rng = self
            .seed
            .map(StdRng::from_seed)
            .unwrap_or_else(StdRng::from_entropy);

        // TODO: Something seems wrong. If test_dir already exists (and at this point
        // all the force restart stuff has been resolved) should I even be doing this?
        let mut node_config = create_single_node_test_config(
            &self.config_path,
            &self.test_config_override,
            &test_dir,
            false,
            false,
            aptos_cached_packages::head_release_bundle(),
            rng,
        )
        .context("Failed to create config for node")?;

        eprintln!();

        // Enable the grpc stream on the node if we will run a txn stream service.
        let run_txn_stream = !self.no_txn_stream;
        node_config.indexer_grpc.enabled = run_txn_stream;
        node_config.indexer_grpc.use_data_service_interface = run_txn_stream;
        node_config
            .indexer_grpc
            .address
            .set_port(self.txn_stream_port);

        // So long as the indexer relies on storage indexing tables, this must be set
        // for the indexer GRPC stream on the node to work.
        node_config.storage.enable_indexer = run_txn_stream;

        let node_api_url = socket_addr_to_url(&node_config.api.address, "http").unwrap();
        let data_service_url =
            socket_addr_to_url(&node_config.indexer_grpc.address, "http").unwrap();

        let faucet_config = if self.no_faucet {
            None
        } else {
            // TODO: --with-faucet doesn't work without --force-restart now. Perhaps
            // related to the changes I made to building the node configs.
            Some(FaucetArgs::build_for_cli(
                node_api_url.clone(),
                self.faucet_port,
                FunderKeyEnum::KeyFile(test_dir.join("mint.key")),
                self.do_not_delegate,
                None,
            ))
        };

        let mut processor_configs = Vec::new();

        if self.with_indexer_api {
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
        }

        Ok(AllConfigs {
            ready_server_config: self.ready_server_args.clone(),
            node_config,
            faucet_config,
            processor_configs,
        })
    }

    /// Run a processor.
    async fn start_processor(
        &self,
        config: GenericConfig<IndexerGrpcProcessorConfig>,
        data_service_url: Url,
    ) -> CliTypedResult<impl Future<Output = ()>> {
        let processor_name = config.server_config.processor_config.name();
        let svc = format!("processor_{}", processor_name);
        // TODO: It's sort of janky that we build these health checkers here and again
        // later. We should build this when we start the fullnode and use it later on
        // in this function / the ready server / etc.
        HealthChecker::DataServiceGrpc(data_service_url)
            .wait(Some(&svc))
            .await?;
        // TODO: Confirm the DB is up.

        // This function starts a runtime with the name of the processor, so the logs
        // from each processor will go to a separate directory.
        Ok(run_server_with_config(config).map(move |result| {
            eprintln!(
                "Processor {} stopped unexpectedly {:#?}",
                processor_name, result
            );
        }))
    }

    /// Run the Indexer API (Hasura).
    // The standard unified Hasura metadata doesn't work if we don't have all the
    // necessary tables populated, which we don't do because some come from the
    // Python processors. So we use a modified metadata. As such, if you update
    // the processors you will likely have to update the metadata too.
    async fn start_indexer_api(
        &self,
        test_dir: PathBuf,
        config: IndexerApiConfig,
        postgres_connection_string: &str,
    ) -> CliTypedResult<impl Future<Output = ()>> {
        let log_dir = test_dir.join("indexer-api");
        create_dir_all(log_dir.as_path())
            .map_err(|err| CliError::IO(format!("Failed to create {}", log_dir.display()), err))?;

        let data = format!(
            "To see logs for the Indexer API run the following command:\n\ndocker logs {}",
            INDEXER_API_CONTAINER_NAME
        );
        std::fs::write(log_dir.join("README.md"), data).expect("Unable to write file");

        // TODO: The processor liveness port doesn't really do anything. It should only
        // return ready when it is actually at the point where it is processing
        // transactions successfully. Once we have that, wait for all the processors.
        // TODO: Look at the old code I had for how I configured Redis and use that,
        // particularly for logging.
        // If we're on Mac, replace 127.0.0.1 with host.docker.internal.
        // https://stackoverflow.com/a/24326540/3846032
        let postgres_connection_string = if cfg!(target_os = "macos") {
            postgres_connection_string.replace("127.0.0.1", "host.docker.internal")
        } else {
            postgres_connection_string.to_string()
        };
        // TODO: Confirm the above works on Linux and Windows.

        // TODO: Try docker run and then docker attach --no-stdin
        // TODO: Consider using --cidfile and making sure to stop any
        // old container that might have been running.

        // TODO: Check for internet access if the image doesn't exist locally.
        // TODO: Pull the image separately in a previous step so it isn't subject
        // to the 30 second timeout.

        // https://stackoverflow.com/q/77171786/3846032
        let child = Command::new("docker")
            .arg("run")
            .arg("-q")
            .arg("--rm")
            .arg("--tty")
            .arg("--no-healthcheck")
            .arg("--name")
            .arg(INDEXER_API_CONTAINER_NAME)
            .arg("-p")
            .arg(format!(
                "{}:{}",
                config.indexer_api_port, config.indexer_api_port
            ))
            .arg("-e")
            .arg(format!("PG_DATABASE_URL={}", postgres_connection_string))
            .arg("-e")
            .arg(format!(
                "HASURA_GRAPHQL_METADATA_DATABASE_URL={}",
                postgres_connection_string
            ))
            .arg("-e")
            .arg(format!(
                "INDEXER_V2_POSTGRES_URL={}",
                postgres_connection_string
            ))
            .arg("-e")
            .arg("HASURA_GRAPHQL_DEV_MODE=true")
            .arg("-e")
            .arg("HASURA_GRAPHQL_ENABLE_CONSOLE=true")
            .arg("-e")
            .arg("HASURA_GRAPHQL_CONSOLE_ASSETS_DIR=/srv/console-assets")
            .arg("-e")
            .arg(format!(
                "HASURA_GRAPHQL_SERVER_PORT={}",
                config.indexer_api_port
            ))
            .arg("hasura/graphql-engine:v2.33.0")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|err| {
                CliError::UnexpectedError(format!("Failed to start indexer API: {}", err))
            })?;

        let fut = async move {
            match child.wait_with_output().await {
                Ok(output) => {
                    // Print nothing, this probably implies ctrl+C.
                    info!("Indexer API stopped with output: {:?}", output);
                },
                Err(err) => {
                    eprintln!("Indexer API stopped unexpectedly with error: {}", err);
                },
            }
        };

        Ok(fut)
    }

    /// Run the ready server.
    async fn start_ready_server(
        &self,
        health_checks: Vec<HealthChecker>,
    ) -> CliTypedResult<impl Future<Output = ()>> {
        let config = self.ready_server_args.clone();
        Ok(run_ready_server(health_checks, config).map(|result| {
            eprintln!("Faucet stopped unexpectedly {:#?}", result);
        }))
    }

    /// Wait for many services to start up. This prints a message like "X is starting,
    /// please wait..." for each service and then "X is running. Endpoint: <url>"
    /// when it's ready.
    async fn wait_for_startup<'a>(&self, health_checks: &Vec<HealthChecker>) -> CliTypedResult<()> {
        let mut futures: Vec<Pin<Box<dyn futures::Future<Output = anyhow::Result<()>> + Send>>> =
            Vec::new();

        for health_check in health_checks {
            // We don't want to print anything for the processors, it'd be too spammy.
            let silent = match health_check {
                HealthChecker::NodeApi(_) => false,
                HealthChecker::Http(_, name) => name.contains("processor"),
                HealthChecker::DataServiceGrpc(_) => false,
            };
            if !silent {
                eprintln!("{} is starting, please wait...", health_check);
            }
            let fut = async move {
                health_check.wait(None).await?;
                if !silent {
                    eprintln!(
                        "{} is running. Endpoint: {}",
                        health_check,
                        health_check.address_str()
                    );
                }
                Ok(())
            };
            futures.push(Box::pin(fut));
        }

        eprintln!();

        // We use join_all because we expect all of these to return.
        for f in futures::future::join_all(futures).await {
            f.map_err(|err| {
                CliError::UnexpectedError(format!(
                    "One of the services failed to start up: {:?}",
                    err
                ))
            })?;
        }

        eprintln!("\nApplying post startup steps...");

        Ok(())
    }
}

#[async_trait]
impl CliCommand<()> for RunLocalTestnet {
    fn command_name(&self) -> &'static str {
        "RunLocalTestnet"
    }

    async fn execute(mut self) -> CliTypedResult<()> {
        if self.postgres_args.postgres_database == "postgres" {
            return Err(CliError::UnexpectedError(
                "The postgres database name cannot be \"postgres\"".to_string(),
            ));
        }

        let global_config = GlobalConfig::load().context("Failed to load global config")?;
        let test_dir = match &self.test_dir {
            Some(test_dir) => test_dir.clone(),
            None => global_config
                .get_config_location(ConfigSearchMode::CurrentDirAndParents)?
                .join(TESTNET_FOLDER),
        };

        // TODO: Run pre steps here, e.g. deleting any existing container.
        let status = Command::new("docker")
            .arg("rm")
            .arg("-f")
            .arg(INDEXER_API_CONTAINER_NAME)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .context("Failed to start container for indexer API")?;

        if !status.success() {
            return Err(CliError::UnexpectedError(format!(
                "Failed to delete any existing container for indexer API: {:?}",
                status
            )));
        }

        // If asked, remove the current test directory and start with a new node.
        if self.force_restart && test_dir.exists() {
            prompt_yes_with_override(
                "Are you sure you want to delete the existing local testnet data?",
                self.prompt_options,
            )?;
            remove_dir_all(test_dir.as_path()).map_err(|err| {
                CliError::IO(format!("Failed to delete {}", test_dir.display()), err)
            })?;
            info!("Deleted test directory at: {:?}", test_dir);
        }

        if !test_dir.exists() {
            info!("Test directory does not exist, creating it: {:?}", test_dir);
            create_dir_all(test_dir.as_path()).map_err(|err| {
                CliError::IO(format!("Failed to create {}", test_dir.display()), err)
            })?;
            info!("Created test directory: {:?}", test_dir);
        }

        // Set up logging for anything that uses tracing. These logs will go to
        // different directories based on the name of the runtime.
        let td = test_dir.clone();
        let make_writer =
            move || ThreadNameMakeWriter::new(td.clone()).make_writer() as Box<dyn std::io::Write>;
        setup_logging(Some(Box::new(make_writer)));

        let all_configs = self
            .build_configs(test_dir.clone())
            .context("Failed to build configs")?;

        // If we're running the indexer stack and using a DB outside of Docker, drop
        // and recreate the database. For this we connect to the postgres database so
        // we can drop the database we'll actually use.
        if self.force_restart && self.use_host_postgres {
            let connection_string = self.postgres_args.get_connection_string(Some("postgres"));
            info!("Dropping database {}", self.postgres_args.postgres_database);
            let pg_pool = new_db_pool(&connection_string).context("Failed to connect to DB")?;
            let mut connection = pg_pool.get().context("Failed to create connection to DB")?;
            recreate_database(&mut connection, &self.postgres_args.postgres_database)
                .with_context(|| {
                    format!("Failed to drop DB {}", self.postgres_args.postgres_database)
                })?;
            info!("Dropped database {}", self.postgres_args.postgres_database);
        }

        let node_api_url = all_configs.get_node_api_url();
        let data_service_url = all_configs.get_data_service_url();

        let AllConfigs {
            ready_server_config,
            node_config,
            faucet_config,
            processor_configs,
        } = all_configs;

        // Collect all the health checks we want to run.
        let mut health_checks = Vec::new();
        health_checks.push(HealthChecker::NodeApi(node_api_url.clone()));

        if let Some(config) = &faucet_config {
            let url = Url::parse(&format!(
                "http://{}:{}",
                config.server_config.listen_address, config.server_config.listen_port
            ))
            .unwrap();
            health_checks.push(HealthChecker::Http(url, "Faucet".to_string()));
        }

        if !self.no_txn_stream {
            health_checks.push(HealthChecker::DataServiceGrpc(data_service_url.clone()));
        }

        for config in &processor_configs {
            health_checks.push(HealthChecker::Http(
                Url::parse(&format!("http://127.0.0.1:{}", config.health_check_port)).unwrap(),
                config.server_config.processor_config.name().to_string(),
            ));
        }

        if self.with_indexer_api {
            health_checks.push(HealthChecker::Http(
                Url::parse(&format!(
                    "http://127.0.0.1:{}/",
                    self.indexer_api_args.indexer_api_port
                ))
                .unwrap(),
                "Indexer API".to_string(),
            ));
        }

        // Build tasks for running each of the services.
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();

        // Push a task to run the ready server.
        tasks.push(tokio::spawn(
            self.start_ready_server(health_checks.clone())
                .await
                .context("Failed to create future to run the ready server")?,
        ));

        // Run the node API.
        tasks.push(tokio::spawn(
            self.start_node(test_dir.clone(), node_config)
                .await
                .context("Failed to create future to run the node")?,
        ));

        // If configured, run the faucet.
        if let Some(config) = faucet_config {
            tasks.push(tokio::spawn(
                self.start_faucet(config, node_api_url.clone())
                    .await
                    .context("Failed to create future to run the faucet")?,
            ));
        }

        // Run each of the indexer processors.
        for config in processor_configs {
            let data_service_url = data_service_url.clone();
            let processor_name = config.server_config.processor_config.name();
            tasks.push(tokio::spawn(
                self.start_processor(config, data_service_url)
                    .await
                    .context(format!(
                        "Failed to create future to run processor {}",
                        processor_name
                    ))?,
            ));
        }

        // Run the indexer API (Hasura).
        if self.with_indexer_api {
            tasks.push(tokio::spawn(
                self.start_indexer_api(
                    test_dir.clone(),
                    self.indexer_api_args.clone(),
                    &self.postgres_args.get_connection_string(None),
                )
                .await
                .context("Failed to create future to run the indexer API")?,
            ));
            tokio::spawn(async move {
                tokio::signal::ctrl_c()
                    .await
                    .context("Failed to await ctrl+C")?;

                eprintln!("\nReceived ctrl+C, stopping spawned services...");

                let status = Command::new("docker")
                    .arg("rm")
                    .arg("-f")
                    .arg(INDEXER_API_CONTAINER_NAME)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .await
                    .context("Failed to stop indexer API")?;

                if !status.success() {
                    return Err(CliError::UnexpectedError(format!(
                        "Failed to stop indexer API: {:?}",
                        status
                    )));
                }

                eprintln!("Done, goodbye!");

                std::process::exit(status.code().unwrap_or(1));

                // This helps the compiler figure out the return type of this closure.
                #[allow(unreachable_code)]
                Ok(())
            });
        }

        eprintln!(
            "Readiness endpoint: http://0.0.0.0:{}/\n",
            ready_server_config.ready_server_listen_port
        );

        // Wait for all the services to start up.
        self.wait_for_startup(&health_checks).await?;

        // Execute post startup steps.
        post_metadata(HASURA_METADATA, self.indexer_api_args.indexer_api_port)
            .await
            .context("Failed to apply Hasura metadata for Indexer API")?;

        eprintln!("\nSetup is complete, you can now use the local testnet!");

        // Wait for all of the futures for the tasks. We should never get past this
        // point unless something goes wrong or the user signals for the process to
        // end.
        let result = futures::future::select_all(tasks).await;

        // TODO: Try to join all of them to see what happened.
        Err(CliError::UnexpectedError(format!(
            "One of the components stopped unexpectedly: {:?}",
            result
        )))
    }
}

// TODO: Consider refactoring so each component has:
//
// - Config creation
// - Running a task
// - Exporting health checks
//
// Before doing this make sure you have a structure that works for the checks and
// whatnot we need to do that check across component boundaries.
//
// If you do this, consider getting rid of the AllConfigs struct.
//
// We need some kind of post healthy hook too, e.g. to apply the hasura metadata.
