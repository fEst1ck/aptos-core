// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    counters,
    scheduler::{DependencyResult, DependencyStatus, Scheduler},
    task::Transaction,
    txn_last_input_output::ReadDescriptor,
};
use aptos_aggregator::{
    delta_change_set::serialize,
    resolver::{AggregatorReadMode, TAggregatorView},
    types::{AggregatorID, AggregatorValue, TryFromMoveValue, TryIntoMoveValue},
};
use aptos_logger::error;
use aptos_mvhashmap::{
    types::{
        MVAggregatorsError, MVDataError, MVDataOutput, MVModulesError, MVModulesOutput, TxnIndex,
    },
    unsync_map::UnsyncMap,
    MVHashMap,
};
use aptos_state_view::{StateViewId, TStateView};
use aptos_types::{
    executable::{Executable, ModulePath},
    state_store::{state_storage_usage::StateStorageUsage, state_value::StateValue},
    write_set::TransactionWrite,
};
use aptos_vm_logging::{log_schema::AdapterLogSchema, prelude::*};
use aptos_vm_types::resolver::{StateStorageView, TModuleView, TResourceView};
use move_core_types::{
    value::{IdentifierMappingKind, MoveTypeLayout},
    vm_status::{StatusCode, VMStatus},
};
use move_vm_types::{
    value_transformation::{
        deserialize_and_replace_values_with_ids, TransformationResult, ValueToIdentifierMapping,
    },
    values::Value,
};
use std::{
    cell::RefCell,
    fmt::Debug,
    rc::Rc,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

/// A struct which describes the result of the read from the proxy. The client
/// can interpret these types to further resolve the reads.
#[derive(Debug)]
pub(crate) enum ReadResult<V> {
    // Successful read of a value.
    Value(Arc<V>),
    // Similar to above, but the value was aggregated and is an integer.
    U128(u128),
    // Read did not return anything.
    Uninitialized,
    // Must half the execution of the calling transaction. This might be because
    // there was an inconsistency in observed speculative state, or dependency
    // waiting indicated that the parallel execution had been halted. The String
    // parameter provides more context (error description / message).
    HaltSpeculativeExecution(String),
}

pub(crate) struct ParallelState<'a, T: Transaction, X: Executable> {
    versioned_map: &'a MVHashMap<T::Key, T::Tag, T::Value, X>,
    scheduler: &'a Scheduler,
    counter: &'a AtomicU32,
    captured_reads: RefCell<Vec<ReadDescriptor<T::Key>>>,
}

impl<'a, T: Transaction, X: Executable> ParallelState<'a, T, X> {
    pub(crate) fn new(
        shared_map: &'a MVHashMap<T::Key, T::Tag, T::Value, X>,
        shared_scheduler: &'a Scheduler,
        shared_counter: &'a AtomicU32,
    ) -> Self {
        Self {
            versioned_map: shared_map,
            scheduler: shared_scheduler,
            counter: shared_counter,
            captured_reads: RefCell::new(Vec::new()),
        }
    }

    fn set_aggregator_v2_value(&self, id: AggregatorID, base_value: AggregatorValue) {
        self.versioned_map
            .aggregators()
            .set_base_value(id, base_value)
    }

    fn read_aggregator_v2_committed_value(
        &self,
        id: AggregatorID,
    ) -> Result<AggregatorValue, MVAggregatorsError> {
        self.versioned_map
            .aggregators()
            .read_latest_committed_value(id)
    }

    // TODO: Actually fill in the logic to record fetched executables, etc.
    fn fetch_module(
        &self,
        key: &T::Key,
        txn_idx: TxnIndex,
    ) -> anyhow::Result<MVModulesOutput<T::Value, X>, MVModulesError> {
        // Register a fake read for the read / write path intersection fallback for modules.
        self.captured_reads
            .borrow_mut()
            .push(ReadDescriptor::from_module(key.clone()));

        self.versioned_map.modules().fetch_module(key, txn_idx)
    }

    /// Captures a read from the VM execution, but not unresolved deltas, as in this case it is the
    /// callers responsibility to set the aggregator's base value and call fetch_data again.
    fn fetch_data(&self, key: &T::Key, txn_idx: TxnIndex) -> ReadResult<T::Value> {
        use MVDataError::*;
        use MVDataOutput::*;

        loop {
            match self.versioned_map.data().fetch_data(key, txn_idx) {
                Ok(Versioned(version, v)) => {
                    self.captured_reads
                        .borrow_mut()
                        .push(ReadDescriptor::from_versioned(key.clone(), version));
                    return ReadResult::Value(v);
                },
                Ok(Resolved(value)) => {
                    self.captured_reads
                        .borrow_mut()
                        .push(ReadDescriptor::from_resolved(key.clone(), value));
                    return ReadResult::U128(value);
                },
                Err(Uninitialized) | Err(Unresolved(_)) => {
                    // The underlying assumption here for not recording anything about the read is
                    // that the caller is expected to initialize the contents and serve the reads
                    // solely via the 'fetch_read' interface. Thus, the later, successful read,
                    // will make the needed recordings.
                    return ReadResult::Uninitialized;
                },
                Err(Dependency(dep_idx)) => {
                    // `self.txn_idx` estimated to depend on a write from `dep_idx`.
                    match self.scheduler.wait_for_dependency(txn_idx, dep_idx) {
                        DependencyResult::Dependency(dep_condition) => {
                            let _timer = counters::DEPENDENCY_WAIT_SECONDS.start_timer();
                            // Wait on a condition variable corresponding to the encountered
                            // read dependency. Once the dep_idx finishes re-execution, scheduler
                            // will mark the dependency as resolved, and then the txn_idx will be
                            // scheduled for re-execution, which will re-awaken cvar here.
                            // A deadlock is not possible due to these condition variables:
                            // suppose all threads are waiting on read dependency, and consider
                            // one with lowest txn_idx. It observed a dependency, so some thread
                            // aborted dep_idx. If that abort returned execution task, by
                            // minimality (lower transactions aren't waiting), that thread would
                            // finish execution unblock txn_idx, contradiction. Otherwise,
                            // execution_idx in scheduler was lower at a time when at least the
                            // thread that aborted dep_idx was alive, and again, since lower txns
                            // than txn_idx are not blocked, so the execution of dep_idx will
                            // eventually finish and lead to unblocking txn_idx, contradiction.
                            let (lock, cvar) = &*dep_condition;
                            let mut dep_resolved = lock.lock();
                            while let DependencyStatus::Unresolved = *dep_resolved {
                                dep_resolved = cvar.wait(dep_resolved).unwrap();
                            }
                            if let DependencyStatus::ExecutionHalted = *dep_resolved {
                                return ReadResult::HaltSpeculativeExecution(
                                    "Speculative error to halt BlockSTM early.".to_string(),
                                );
                            }
                        },
                        DependencyResult::ExecutionHalted => {
                            return ReadResult::HaltSpeculativeExecution(
                                "Speculative error to halt BlockSTM early.".to_string(),
                            );
                        },
                        DependencyResult::Resolved => continue,
                    }
                },
                Err(DeltaApplicationFailure) => {
                    // Delta application failure currently should never happen. Here, we assume it
                    // happened because of speculation and return 0 to the Move-VM. Validation will
                    // ensure the transaction re-executes if 0 wasn't the right number.

                    self.captured_reads
                        .borrow_mut()
                        .push(ReadDescriptor::from_speculative_failure(key.clone()));

                    return ReadResult::HaltSpeculativeExecution(
                        "Delta application failure (must be speculative)".to_string(),
                    );
                },
            };
        }
    }
}

pub(crate) struct SequentialState<'a, T: Transaction, X: Executable> {
    pub(crate) unsync_map: &'a UnsyncMap<T::Key, T::Value, X>,
    pub(crate) counter: Rc<RefCell<u32>>,
}

pub(crate) enum ViewState<'a, T: Transaction, X: Executable> {
    Sync(ParallelState<'a, T, X>),
    Unsync(SequentialState<'a, T, X>),
}

/// A struct that represents a single block execution worker thread's view into the state,
/// some of which (in Sync case) might be shared with other workers / threads. By implementing
/// all necessary traits, LatestView is provided to the VM and used to intercept the reads.
/// In the Sync case, also records captured reads for later validation. latest_txn_idx
/// must be set according to the latest transaction that the worker was / is executing.
pub(crate) struct LatestView<'a, T: Transaction, S: TStateView<Key = T::Key>, X: Executable> {
    base_view: &'a S,
    latest_view: ViewState<'a, T, X>,
    txn_idx: TxnIndex,
}

impl<'a, T: Transaction, S: TStateView<Key = T::Key>, X: Executable> LatestView<'a, T, S, X> {
    pub(crate) fn new(
        base_view: &'a S,
        latest_view: ViewState<'a, T, X>,
        txn_idx: TxnIndex,
    ) -> Self {
        Self {
            base_view,
            latest_view,
            txn_idx,
        }
    }

    /// Drains the captured reads.
    pub(crate) fn take_reads(&self) -> Vec<ReadDescriptor<T::Key>> {
        match &self.latest_view {
            ViewState::Sync(state) => state.captured_reads.take(),
            ViewState::Unsync(_) => {
                unreachable!("Take reads called in sequential setting (not captured)")
            },
        }
    }

    fn get_base_value(&self, state_key: &T::Key) -> anyhow::Result<Option<StateValue>> {
        let ret = self.base_view.get_state_value(state_key);

        if ret.is_err() {
            // Even speculatively, reading from base view should not return an error.
            // Thus, this critical error log and count does not need to be buffered.
            let log_context = AdapterLogSchema::new(self.base_view.id(), self.txn_idx as usize);
            alert!(
                log_context,
                "[VM, StateView] Error getting data from storage for {:?}",
                state_key
            );
        }
        ret
    }

    /// Given a state value, performs deserialization-serialization round-trip
    /// to replace any aggregator / snapshot values.
    fn replace_values_with_identifiers(
        &self,
        state_value: StateValue,
        layout: &MoveTypeLayout,
    ) -> anyhow::Result<StateValue> {
        state_value.map_bytes(|bytes| {
            // This call will replace all occurrences of aggregator / snapshot
            // values with unique identifiers with the same type layout.
            // The values are stored in aggregators multi-version data structure,
            // see the actual trait implementation for more details.
            let patched_value =
                deserialize_and_replace_values_with_ids(bytes.as_ref(), layout, self).ok_or_else(
                    || anyhow::anyhow!("Failed to deserialize resource during id replacement"),
                )?;
            patched_value
                .simple_serialize(layout)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Failed to serialize value {} after id replacement",
                        patched_value
                    )
                })
                .map(|b| b.into())
        })
    }
}

impl<'a, T: Transaction, S: TStateView<Key = T::Key>, X: Executable> TResourceView
    for LatestView<'a, T, S, X>
{
    type Key = T::Key;
    type Layout = MoveTypeLayout;

    fn get_resource_state_value(
        &self,
        state_key: &Self::Key,
        maybe_layout: Option<&Self::Layout>,
    ) -> anyhow::Result<Option<StateValue>> {
        debug_assert!(
            state_key.module_path().is_none(),
            "Reading a module {:?} using ResourceView",
            state_key,
        );

        match &self.latest_view {
            ViewState::Sync(state) => {
                let mut mv_value = state.fetch_data(state_key, self.txn_idx);

                if matches!(mv_value, ReadResult::Uninitialized) {
                    let from_storage = self.base_view.get_state_value(state_key)?;
                    let maybe_patched_from_storage = match (from_storage, maybe_layout) {
                        // There are aggregators / aggregator snapshots in the
                        // resource, so we have to replace the actual values with
                        // identifiers.
                        // TODO(aggregator): gate by the flag.
                        (Some(state_value), Some(layout)) => {
                            let res = self.replace_values_with_identifiers(state_value, layout);
                            if let Err(err) = &res {
                                // TODO(aggregator): This means replacement failed
                                //       and most likely there is a bug. Log the error
                                //       for now, and add recovery mechanism later.
                                let log_context = AdapterLogSchema::new(
                                    self.base_view.id(),
                                    self.txn_idx as usize,
                                );
                                alert!(
                                    log_context,
                                    "[VM, ResourceView] Error during value to id replacement for {:?}: {}",
                                    state_key,
                                    err
                                );
                            }
                            Some(res?)
                        },
                        (from_storage, _) => from_storage,
                    };

                    // This base value can also be used to resolve AggregatorV1 directly from
                    // the versioned data-structure (without more storage calls).
                    state.versioned_map.data().provide_base_value(
                        state_key.clone(),
                        TransactionWrite::from_state_value(maybe_patched_from_storage),
                    );

                    mv_value = state.fetch_data(state_key, self.txn_idx);
                }

                match mv_value {
                    ReadResult::Value(v) => Ok(v.as_state_value()),
                    ReadResult::U128(v) => Ok(Some(StateValue::new_legacy(serialize(&v).into()))),
                    // ExecutionHalted indicates that the parallel execution is halted.
                    // The read should return immediately and log the error.
                    // For now we use STORAGE_ERROR as the VM will not log the speculative error,
                    // so no actual error will be logged once the execution is halted and
                    // the speculative logging is flushed.
                    ReadResult::HaltSpeculativeExecution(msg) => Err(anyhow::Error::new(
                        VMStatus::error(StatusCode::STORAGE_ERROR, Some(msg)),
                    )),
                    ReadResult::Uninitialized => {
                        unreachable!("base value must already be recorded in the MV data structure")
                    },
                }
            },
            ViewState::Unsync(state) => state.unsync_map.fetch_data(state_key).map_or_else(
                || {
                    // TODO: AggregatorV2 ID for sequential must be replaced in this flow.
                    self.get_base_value(state_key)
                },
                |v| Ok(v.as_state_value()),
            ),
        }
    }

    // TODO: implement here fn get_resource_state_value_metadata & resource_exists.
}

impl<'a, T: Transaction, S: TStateView<Key = T::Key>, X: Executable> TModuleView
    for LatestView<'a, T, S, X>
{
    type Key = T::Key;

    fn get_module_state_value(&self, state_key: &Self::Key) -> anyhow::Result<Option<StateValue>> {
        debug_assert!(
            state_key.module_path().is_some(),
            "Reading a resource {:?} using ModuleView",
            state_key,
        );

        match &self.latest_view {
            ViewState::Sync(state) => {
                use MVModulesError::*;
                use MVModulesOutput::*;

                match state.fetch_module(state_key, self.txn_idx) {
                    Ok(Executable(_)) => unreachable!("Versioned executable not implemented"),
                    Ok(Module((v, _))) => Ok(v.as_state_value()),
                    Err(Dependency(_)) => {
                        // Return anything (e.g. module does not exist) to avoid waiting,
                        // because parallel execution will fall back to sequential anyway.
                        Ok(None)
                    },
                    Err(NotFound) => self.base_view.get_state_value(state_key),
                }
            },
            ViewState::Unsync(state) => state.unsync_map.fetch_data(state_key).map_or_else(
                || self.get_base_value(state_key),
                |v| Ok(v.as_state_value()),
            ),
        }
    }
}

impl<'a, T: Transaction, S: TStateView<Key = T::Key>, X: Executable> StateStorageView
    for LatestView<'a, T, S, X>
{
    fn id(&self) -> StateViewId {
        self.base_view.id()
    }

    fn get_usage(&self) -> anyhow::Result<StateStorageUsage> {
        self.base_view.get_usage()
    }
}

impl<'a, T: Transaction, S: TStateView<Key = T::Key>, X: Executable> TAggregatorView
    for LatestView<'a, T, S, X>
{
    type IdentifierV1 = T::Key;
    type IdentifierV2 = AggregatorID;

    fn get_aggregator_v1_state_value(
        &self,
        state_key: &Self::IdentifierV1,
        _mode: AggregatorReadMode,
    ) -> anyhow::Result<Option<StateValue>> {
        // TODO: Integrate aggregators V1. That is, we can lift the u128 value
        //       from the state item by passing the right layout here. This can
        //       be useful for cross-testing the old and the new flows.
        // self.get_resource_state_value(state_key, Some(&MoveTypeLayout::U128))
        self.get_resource_state_value(state_key, None)
    }

    fn generate_aggregator_v2_id(&self) -> Self::IdentifierV2 {
        match &self.latest_view {
            ViewState::Sync(state) => (state.counter.fetch_add(1, Ordering::SeqCst) as u64).into(),
            ViewState::Unsync(state) => {
                let mut counter = state.counter.borrow_mut();
                let id = (*counter as u64).into();
                *counter += 1;
                id
            },
        }
    }
}

// For aggregators V2, values are replaced with identifiers at deserialization time,
// and are replaced back when the value is serialized. The "lifted" values are cached
// by the `LatestView` in the aggregators multi-version data structure.
impl<'a, T: Transaction, S: TStateView<Key = T::Key>, X: Executable> ValueToIdentifierMapping
    for LatestView<'a, T, S, X>
{
    fn value_to_identifier(
        &self,
        kind: &IdentifierMappingKind,
        layout: &MoveTypeLayout,
        value: Value,
    ) -> TransformationResult<Value> {
        let id = self.generate_aggregator_v2_id();
        match &self.latest_view {
            ViewState::Sync(state) => {
                let base_value = AggregatorValue::try_from_move_value(layout, value, Some(kind))?;
                state.set_aggregator_v2_value(id, base_value)
            },
            ViewState::Unsync(_state) => {
                // TODO(aggregator): Support sequential execution.
                unimplemented!("Value to ID replacement for sequential execution is not supported")
            },
        };
        Ok(id.try_into_move_value(layout)?)
    }

    fn identifier_to_value(
        &self,
        layout: &MoveTypeLayout,
        identifier_value: Value,
    ) -> TransformationResult<Value> {
        let id = AggregatorID::try_from_move_value(layout, identifier_value, /*hint=*/ None)?;
        match &self.latest_view {
            ViewState::Sync(state) => Ok(state
                .read_aggregator_v2_committed_value(id)
                .expect("Committed value for ID must always exist")
                .try_into_move_value(layout)?),
            ViewState::Unsync(_state) => {
                // TODO(aggregator): Support sequential execution.
                unimplemented!("ID to value replacement for sequential execution is not supported")
            },
        }
    }
}
