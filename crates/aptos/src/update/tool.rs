// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

<<<<<<< HEAD
// use super::{aptos::AptosUpdateTool, revela::RevelaUpdateTool};
use crate::{
    common::types::{CliCommand, CliResult},
    update::movefmt::FormatterUpdateTool,
=======
use super::{aptos::AptosUpdateTool, revela::RevelaUpdateTool};
use crate::{
    common::types::{CliCommand, CliResult},
    update::{
        move_mutation_test::MutationTestUpdaterTool, movefmt::FormatterUpdateTool,
        prover_dependencies::ProverDependencyInstaller,
    },
>>>>>>> aptos-framework-v1.34.0
};
use clap::Subcommand;

/// Update the CLI or other tools it depends on.
#[derive(Subcommand)]
pub enum UpdateTool {
<<<<<<< HEAD
    // Aptos(AptosUpdateTool),
    // Revela(RevelaUpdateTool),
    Movefmt(FormatterUpdateTool),
=======
    Aptos(AptosUpdateTool),
    Revela(RevelaUpdateTool),
    Movefmt(FormatterUpdateTool),
    MoveMutationTest(MutationTestUpdaterTool),
    ProverDependencies(ProverDependencyInstaller),
>>>>>>> aptos-framework-v1.34.0
}

impl UpdateTool {
    pub async fn execute(self) -> CliResult {
        match self {
<<<<<<< HEAD
            // UpdateTool::Aptos(tool) => tool.execute_serialized().await,
            // UpdateTool::Revela(tool) => tool.execute_serialized().await,
            UpdateTool::Movefmt(tool) => tool.execute_serialized().await,
=======
            UpdateTool::Aptos(tool) => tool.execute_serialized().await,
            UpdateTool::Revela(tool) => tool.execute_serialized().await,
            UpdateTool::Movefmt(tool) => tool.execute_serialized().await,
            UpdateTool::MoveMutationTest(tool) => tool.execute_serialized().await,
            UpdateTool::ProverDependencies(tool) => tool.execute_serialized().await,
>>>>>>> aptos-framework-v1.34.0
        }
    }
}
