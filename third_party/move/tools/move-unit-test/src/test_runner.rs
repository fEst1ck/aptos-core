// Copyright (c) The Diem Core Contributors
// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use crate::{
    extensions, format_module_id,
    test_reporter::{
        FailureReason, MoveError, TestFailure, TestResults, TestRunInfo, TestStatistics,
        UnitTestFactory,
    },
};
use anyhow::Result;
use colored::*;
use legacy_move_compiler::unit_test::{
    ExpectedFailure, ModuleTestPlan, NamedOrBytecodeModule, TestCase, TestPlan,
};
use move_binary_format::{
    errors::{Location, PartialVMError, VMResult},
    file_format::CompiledModule,
};
use move_bytecode_utils::Modules;
use move_core_types::{
    account_address::AccountAddress,
    effects::{ChangeSet, Op},
    identifier::IdentStr,
    language_storage::ModuleId,
    value::{serialize_values, MoveValue, TestArg},
    vm_status::StatusCode,
};
use move_resource_viewer::MoveValueAnnotator;
use move_vm_runtime::{
    data_cache::TransactionDataCache,
    module_traversal::{TraversalContext, TraversalStorage},
    move_vm::MoveVM,
    native_extensions::NativeContextExtensions,
    native_functions::NativeFunctionTable,
    AsFunctionValueExtension, AsUnsyncModuleStorage, ModuleStorage, RuntimeEnvironment,
};
use move_vm_test_utils::InMemoryStorage;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rayon::prelude::*;
use std::{env, io::Write, marker::Send, sync::Mutex, time::Instant};

/// Test state common to all tests
pub struct SharedTestingConfig {
    save_storage_state_on_failure: bool,
    report_stacktrace_on_abort: bool,
    starting_storage_state: InMemoryStorage,
    #[allow(dead_code)] // used by some features
    source_files: Vec<String>,
    record_writeset: bool,
}

pub struct TestRunner {
    num_threads: usize,
    testing_config: SharedTestingConfig,
    tests: TestPlan,
}

/// Setup storage state with the set of modules that will be needed for all tests
fn setup_test_storage<'a>(
    modules: impl Iterator<Item = &'a CompiledModule>,
    runtime_environment: RuntimeEnvironment,
) -> Result<InMemoryStorage> {
    let mut storage = InMemoryStorage::new_with_runtime_environment(runtime_environment);
    let modules = Modules::new(modules);
    for module in modules
        .compute_dependency_graph()
        .compute_topological_order()?
    {
        let mut module_bytes = Vec::new();
        module.serialize_for_version(Some(module.version), &mut module_bytes)?;
        storage.add_module_bytes(module.self_addr(), module.self_name(), module_bytes.into());
    }

    Ok(storage)
}

/// Print the updates to storage represented by `cs` in the context of the starting storage state
/// `storage`.
fn print_resources_and_extensions(
    cs: &ChangeSet,
    extensions: &mut NativeContextExtensions,
    storage: &InMemoryStorage,
) -> Result<String> {
    use std::fmt::Write;
    let mut buf = String::new();
    let annotator = MoveValueAnnotator::new(storage.clone());
    for (account_addr, account_state) in cs.accounts() {
        writeln!(&mut buf, "0x{}:", account_addr.short_str_lossless())?;

        for (tag, resource_op) in account_state.resources() {
            if let Op::New(resource) | Op::Modify(resource) = resource_op {
                writeln!(
                    &mut buf,
                    "\t{}",
                    format!("=> {}", annotator.view_resource(tag, resource)?).replace('\n', "\n\t")
                )?;
            }
        }
    }

    let module_storage = storage.as_unsync_module_storage();
    let function_value_extension = module_storage.as_function_value_extension();
    extensions::print_change_sets(&mut buf, extensions, &function_value_extension);

    Ok(buf)
}

impl TestRunner {
    pub fn new(
        num_threads: usize,
        save_storage_state_on_failure: bool,
        report_stacktrace_on_abort: bool,
        tests: TestPlan,
        // TODO: maybe we should require the clients to always pass in a list of native functions so
        // we don't have to make assumptions about their gas parameters.
        native_function_table: Option<NativeFunctionTable>,
        genesis_state: Option<ChangeSet>,
        record_writeset: bool,
    ) -> Result<Self> {
        let native_function_table = native_function_table.unwrap_or_else(|| {
            move_stdlib::natives::all_natives(
                AccountAddress::from_hex_literal("0x1").unwrap(),
                move_stdlib::natives::GasParameters::zeros(),
            )
        });
        let runtime_environment = RuntimeEnvironment::new(native_function_table);

        let source_files = tests
            .files
            .values()
            .map(|(filepath, _)| filepath.to_string())
            .collect();
        let modules = tests.module_info.values().map(|info| match info {
            NamedOrBytecodeModule::Named(named_compiled_module) => &named_compiled_module.module,
            NamedOrBytecodeModule::Bytecode(compiled_module) => compiled_module,
        });
        let mut starting_storage_state = setup_test_storage(modules, runtime_environment)?;
        if let Some(genesis_state) = genesis_state {
            starting_storage_state.apply(genesis_state)?;
        }

        Ok(Self {
            testing_config: SharedTestingConfig {
                save_storage_state_on_failure,
                report_stacktrace_on_abort,
                starting_storage_state,
                source_files,
                record_writeset,
            },
            num_threads,
            tests,
        })
    }

    pub fn run<W: Write + Send, F: UnitTestFactory + Send>(
        self,
        writer: &Mutex<W>,
        options: &Mutex<F>,
    ) -> Result<TestResults> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap()
            .install(|| {
                let final_statistics = self
                    .tests
                    .module_tests
                    .par_iter()
                    .map(|(_, test_plan)| {
                        self.testing_config
                            .exec_module_tests(test_plan, writer, options)
                    })
                    .reduce(TestStatistics::new, |acc, stats| acc.combine(stats));

                Ok(TestResults::new(final_statistics, self.tests))
            })
    }

    pub fn filter(&mut self, test_name_slice: &str) {
        for (module_id, module_test) in self.tests.module_tests.iter_mut() {
            if module_id.name().as_str().contains(test_name_slice) {
                continue;
            } else {
                let tests = std::mem::take(&mut module_test.tests);
                module_test.tests = tests
                    .into_iter()
                    .filter(|(test_name, _)| {
                        let full_name =
                            format!("{}::{}", module_id.name().as_str(), test_name.as_str());
                        full_name.contains(test_name_slice)
                    })
                    .collect();
            }
        }
    }
}

// TODO: do not expose this to backend implementations
struct TestOutput<'a, 'b, W> {
    test_plan: &'a ModuleTestPlan,
    writer: &'b Mutex<W>,
}

impl<W: Write> TestOutput<'_, '_, W> {
    fn pass(&self, fn_name: &str) {
        writeln!(
            self.writer.lock().unwrap(),
            "[ {}    ] {}::{}",
            "PASS".bold().bright_green(),
            format_module_id(&self.test_plan.module_id),
            fn_name
        )
        .unwrap()
    }

    fn fail(&self, fn_name: &str) {
        writeln!(
            self.writer.lock().unwrap(),
            "[ {}    ] {}::{}",
            "FAIL".bold().bright_red(),
            format_module_id(&self.test_plan.module_id),
            fn_name,
        )
        .unwrap()
    }

    fn timeout(&self, fn_name: &str) {
        writeln!(
            self.writer.lock().unwrap(),
            "[ {} ] {}::{}",
            "TIMEOUT".bold().bright_yellow(),
            format_module_id(&self.test_plan.module_id),
            fn_name,
        )
        .unwrap();
    }
}

impl SharedTestingConfig {
    /// Execute a function call and return its return value.
    /// Used for generating test arguments via function calls like `#[test(a = gen())]`.
    fn execute_function_call<F: UnitTestFactory>(
        &self,
        module_id: &ModuleId,
        function_name: &str,
        factory: &Mutex<F>,
        extensions: &mut NativeContextExtensions,
    ) -> VMResult<MoveValue> {
        let module_storage = self.starting_storage_state.as_unsync_module_storage();
        let mut gas_meter = factory.lock().unwrap().new_gas_meter();
        let traversal_storage = TraversalStorage::new();
        let mut traversal_context = TraversalContext::new(&traversal_storage);
        let mut data_cache = TransactionDataCache::empty();

        let ident_str = IdentStr::new(function_name)
            .map_err(|e| PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                .with_message(format!("Invalid function name '{}': {}", function_name, e))
                .finish(Location::Undefined))?;
        
        let function = module_storage.load_function(
            module_id,
            ident_str,
            &[],
        )?;

        let result = MoveVM::execute_loaded_function(
            function,
            Vec::<Vec<u8>>::new(), // No arguments for generator functions
            &mut data_cache,
            &mut gas_meter,
            &mut traversal_context,
            extensions,
            &module_storage,
            &self.starting_storage_state,
        )?;

        // Extract the return value (generator functions should return a single value)
        if result.return_values.len() != 1 {
            return Err(PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                .with_message(format!("Generator function {} must return exactly one value", function_name))
                .finish(Location::Undefined));
        }

        let (bytes, layout) = &result.return_values[0];
        MoveValue::simple_deserialize(bytes, layout)
            .map_err(|e| PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                .with_message(format!("Failed to deserialize return value from {}: {:?}", function_name, e))
                .finish(Location::Undefined))
    }

    #[allow(clippy::field_reassign_with_default)]
    fn execute_via_move_vm<F: UnitTestFactory>(
        &self,
        test_plan: &ModuleTestPlan,
        function_name: &str,
        test_args: &[MoveValue],
        factory: &Mutex<F>,
    ) -> (
        VMResult<ChangeSet>,
        VMResult<NativeContextExtensions>,
        VMResult<Vec<Vec<u8>>>,
        TestRunInfo,
    ) {
        let module_storage = self.starting_storage_state.as_unsync_module_storage();

        let mut extensions = extensions::new_extensions();
        let mut gas_meter = factory.lock().unwrap().new_gas_meter();
        let traversal_storage = TraversalStorage::new();
        let mut traversal_context = TraversalContext::new(&traversal_storage);
        let mut data_cache = TransactionDataCache::empty();

        // TODO: collect VM logs if the verbose flag (i.e, `self.verbose`) is set

        let now = Instant::now();
        let result = module_storage
            .load_function(
                &test_plan.module_id,
                IdentStr::new(function_name).unwrap(),
                // No type args for now.
                &[],
            )
            .and_then(|function| {
                let args = serialize_values(test_args);
                MoveVM::execute_loaded_function(
                    function,
                    args,
                    &mut data_cache,
                    &mut gas_meter,
                    &mut traversal_context,
                    &mut extensions,
                    &module_storage,
                    &self.starting_storage_state,
                )
            });

        let mut return_result = result.map(|res| {
            res.return_values
                .into_iter()
                .map(|(bytes, _layout)| bytes)
                .collect()
        });
        if !self.report_stacktrace_on_abort {
            if let Err(err) = &mut return_result {
                err.remove_exec_state();
            }
        }

        let test_run_info = TestRunInfo::new(function_name.to_string(), now.elapsed());

        let result = data_cache
            .into_effects(&module_storage)
            .map_err(|err| err.finish(Location::Undefined));
        match result {
            Ok(change_set) => {
                let finalized_test_run_info = factory.lock().unwrap().finalize_test_run_info(
                    &change_set,
                    &mut extensions,
                    gas_meter,
                    test_run_info,
                );

                (
                    Ok(change_set),
                    Ok(extensions),
                    return_result,
                    finalized_test_run_info,
                )
            },
            Err(err) => (Err(err.clone()), Err(err), return_result, test_run_info),
        }
    }

    fn run_one_test<F: UnitTestFactory>(
        &self,
        test_plan: &ModuleTestPlan,
        test_info: &TestCase,
        output: &TestOutput<impl Write>,
        factory: &Mutex<F>,
        stats: &mut TestStatistics,
        rng: &mut StdRng,
        function_name: &String,
        record_test: bool,
        extensions: &mut NativeContextExtensions,
    ) -> bool {
        // Generate test arguments, handling function calls
        let mut args = Vec::new();
        let mut data = vec![0u8; 1024];
        rng.fill(&mut data[..]);
        let mut u = arbitrary::Unstructured::new(&data);

        for arg in &test_info.arguments {
            let value = match arg {
                TestArg::Value(value) => value.clone(),
                TestArg::Constraint(constraint) => constraint.gen(&mut u).expect("Failed to generate test argument"),
                TestArg::FunctionCall(opt_module_id, func_name) => {
                    // Execute the function call to generate the argument
                    let module_id = opt_module_id.as_ref().unwrap_or(&test_plan.module_id);
                    match self.execute_function_call(module_id, func_name, factory, extensions) {
                        Ok(value) => value,
                        Err(err) => {
                            output.fail(function_name);
                            eprintln!("Failed to execute generator function {}::{}: {:?}", 
                                module_id, func_name, err);
                            return false;
                        },
                    }
                },
            };
            args.push(value);
        }

        let failure_input = if test_info.is_prop_test() {
            Some(args.clone())
        } else {
            None
        };
        let (cs_result, ext_result, exec_result, test_run_info) =
            self.execute_via_move_vm(test_plan, function_name, &args, factory);

        if self.record_writeset {
            stats.test_output(
                function_name.to_string(),
                test_plan,
                format!("{:?}", cs_result),
            );
        }

        let save_session_state = || {
            if self.save_storage_state_on_failure {
                cs_result.ok().and_then(|changeset| {
                    ext_result.ok().and_then(|mut extensions| {
                        print_resources_and_extensions(
                            &changeset,
                            &mut extensions,
                            &self.starting_storage_state,
                        )
                        .ok()
                    })
                })
            } else {
                None
            }
        };

        match exec_result {
            Err(err) => {
                let actual_err = MoveError(
                    err.major_status(),
                    err.sub_status(),
                    err.location().clone(),
                    err.message().cloned(),
                );
                assert!(err.major_status() != StatusCode::EXECUTED);
                match test_info.expected_failure.as_ref() {
                    Some(ExpectedFailure::Expected) => {
                        if record_test {
                            output.pass(function_name);
                            stats.test_success(test_run_info, test_plan);
                        }
                        true
                    },
                    Some(ExpectedFailure::ExpectedWithError(expected_err))
                        if expected_err == &actual_err =>
                    {
                        if record_test {
                            output.pass(function_name);
                            stats.test_success(test_run_info, test_plan);
                        }
                        true
                    },
                    Some(ExpectedFailure::ExpectedWithCodeDEPRECATED(code))
                        if actual_err.0 == StatusCode::ABORTED
                            && actual_err.1.is_some()
                            && actual_err.1.unwrap() == *code =>
                    {
                        if record_test {
                            output.pass(function_name);
                            stats.test_success(test_run_info, test_plan);
                        }
                        true
                    },
                    // incorrect cases
                    Some(ExpectedFailure::ExpectedWithError(expected_err)) => {
                        output.fail(function_name);
                        stats.test_failure(
                            TestFailure::new(
                                FailureReason::wrong_error(expected_err.clone(), actual_err),
                                failure_input,
                                test_run_info,
                                Some(err),
                                save_session_state(),
                            ),
                            test_plan,
                        );
                        false
                    },
                    Some(ExpectedFailure::ExpectedWithCodeDEPRECATED(expected_code)) => {
                        output.fail(function_name);
                        stats.test_failure(
                            TestFailure::new(
                                FailureReason::wrong_abort_deprecated(*expected_code, actual_err),
                                failure_input,
                                test_run_info,
                                Some(err),
                                save_session_state(),
                            ),
                            test_plan,
                        );
                        false
                    },
                    None if err.major_status() == StatusCode::OUT_OF_GAS => {
                        // Ran out of ticks, report a test timeout and log a test failure
                        output.timeout(function_name);
                        stats.test_failure(
                            TestFailure::new(
                                FailureReason::timeout(),
                                failure_input,
                                test_run_info,
                                Some(err),
                                save_session_state(),
                            ),
                            test_plan,
                        );
                        false
                    },
                    None => {
                        output.fail(function_name);
                        stats.test_failure(
                            TestFailure::new(
                                FailureReason::unexpected_error(actual_err),
                                failure_input,
                                test_run_info,
                                Some(err),
                                save_session_state(),
                            ),
                            test_plan,
                        );
                        false
                    },
                }
            },
            Ok(_) => {
                // Expected the test to fail, but it executed
                if test_info.expected_failure.is_some() {
                    output.fail(function_name);
                    stats.test_failure(
                        TestFailure::new(
                            FailureReason::no_error(),
                            failure_input,
                            test_run_info,
                            None,
                            save_session_state(),
                        ),
                        test_plan,
                    );
                    false
                } else {
                    // Expected the test to execute fully and it did
                    if record_test {
                        output.pass(function_name);
                        stats.test_success(test_run_info, test_plan);
                    }
                    true
                }
            },
        }
    }

    fn exec_module_tests_move_vm_and_stackless_vm<F: UnitTestFactory>(
        &self,
        test_plan: &ModuleTestPlan,
        output: &TestOutput<impl Write>,
        factory: &Mutex<F>,
    ) -> TestStatistics {
        let mut stats = TestStatistics::new();

        let mut rng = if let Ok(seed_str) = env::var("TEST_SEED") {
            StdRng::seed_from_u64(seed_str.parse::<u64>().unwrap())
        } else {
            StdRng::from_entropy()
        };

        for (function_name, test_info) in &test_plan.tests {
            let repeats = if test_info.is_prop_test() {
                // Use repeats from test case if specified, otherwise fall back to env var or default
                test_info.repeats
                    .or_else(|| {
                        env::var("TEST_REPEAT")
                            .ok()
                            .and_then(|s| s.parse::<usize>().ok())
                    })
                    .unwrap_or(256)
            } else {
                1
            };
            let mut extensions = extensions::new_extensions();
            for _ in 0..repeats - 1 {
                if !self.run_one_test(test_plan, test_info, output, factory, &mut stats, &mut rng, function_name, false, &mut extensions) {
                    return stats;
                }
            }
            self.run_one_test(test_plan, test_info, output, factory, &mut stats, &mut rng, function_name, true, &mut extensions);
        }

        stats
    }

    fn exec_module_tests<F: UnitTestFactory>(
        &self,
        test_plan: &ModuleTestPlan,
        writer: &Mutex<impl Write>,
        factory: &Mutex<F>,
    ) -> TestStatistics {
        let output = TestOutput { test_plan, writer };
        self.exec_module_tests_move_vm_and_stackless_vm(test_plan, &output, factory)
    }
}
