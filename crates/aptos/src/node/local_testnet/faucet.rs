// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{health_checker::HealthChecker, types::ServiceManager, RunLocalTestnet};
use anyhow::Result;
use aptos_faucet_core::server::{FunderKeyEnum, RunConfig};
use async_trait::async_trait;
use clap::Parser;
use futures::{Future, FutureExt};
use reqwest::Url;
use std::path::PathBuf;

/// TODO:
#[derive(Debug, Parser)]
pub struct FaucetArgs {
    /// Do not run a faucet alongside the node.
    ///
    /// Running a faucet alongside the node allows you to create and fund accounts
    /// for testing.
    #[clap(long)]
    pub no_faucet: bool,

    /// This does nothing, we already run a faucet by default. We only keep this here
    /// for backwards compatibility with tests. We will remove this once the commit
    /// that added --no-faucet makes its way to the testnet branch.
    #[clap(long, hide = true)]
    pub with_faucet: bool,

    /// Port to run the faucet on.
    ///
    /// When running, you'll be able to use the faucet at `http://127.0.0.1:<port>/mint` e.g.
    /// `http//127.0.0.1:8081/mint`
    #[clap(long, default_value_t = 8081)]
    pub faucet_port: u16,

    /// Disable the delegation of faucet minting to a dedicated account.
    #[clap(long)]
    pub do_not_delegate: bool,
}

#[derive(Debug)]
pub struct FaucetManager {
    config: RunConfig,
}

impl FaucetManager {
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
impl ServiceManager for FaucetManager {
    fn get_name(&self) -> String {
        "Faucet".to_string()
    }

    fn get_healthchecks(&self) -> Vec<HealthChecker> {
        vec![HealthChecker::Http(
            Url::parse(&format!(
                "http://127.0.0.1:{}",
                self.config.server_config.listen_port
            ))
            .unwrap(),
            "Faucet".to_string(),
        )]
    }

    async fn run_service(&self) -> Result<Box<dyn Future<Output = ()>>> {
        let future = self.config.run().map(|result| {
            eprintln!("Faucet stopped unexpectedly {:#?}", result);
        });
        Ok(Box::new(future))
    }
}
