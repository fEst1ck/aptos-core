// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{health_checker::HealthChecker, RunLocalTestnet};
use anyhow::Result;
use async_trait::async_trait;
use futures::Future;
use std::{fmt::Debug, path::PathBuf};

#[async_trait]
pub trait ServiceManager: Debug {
    fn get_name(&self) -> String;

    /// This is called before the service is run. This is a good place to do any
    /// setup that needs to be done before the service is run.
    async fn pre_run(&self) -> Result<()> {
        Ok(())
    }

    /// All services should expose some way to check if they are healthy. This function
    /// returns HealthCheckers, a struct that serves this purpose, that later services
    /// can use to make sure prerequisite services have started. These are also used
    /// by the "ready server", a server that exposes a unified endpoint for checking
    /// if all services are ready.
    fn get_healthchecks(&self) -> Vec<HealthChecker>;

    /// This wrapper just handles using the health checkers before running the service.
    async fn run(
        &self,
        prerequisite_health_checkers: Vec<HealthChecker>,
    ) -> Result<Box<dyn Future<Output = ()>>> {
        for health_checker in prerequisite_health_checkers {
            health_checker.wait(Some(&self.get_name())).await?;
        }
        self.run_service().await
    }

    // It would be better to use `impl Future<Output = ()>` here, which you can do in
    // a non trait function, but it is not possible right now. See the following issue:
    // https://github.com/rust-lang/rust/issues/91611
    //
    /// This is the main function that runs the service. It should return a future
    /// that we wait on. This future should never end. Because this future has no
    /// return type, it is recommended to map the error and print it to stderr.
    ///
    /// Note for the reader. This function is async, but that is totally unrelated to
    /// the fact that this function returns a future. We make this function async so
    /// that it can do necessary setup prior to running the future, which may be async.
    /// Similarly, this function returns a Result, but the future itself does not.
    async fn run_service(&self) -> Result<Box<dyn Future<Output = ()>>>;

    /// This is called after the service is healthy. This is a good place to do any
    /// setup that needs to be done after the service is healthy.
    async fn post_healthy(&self) -> Result<()> {
        Ok(())
    }
}
