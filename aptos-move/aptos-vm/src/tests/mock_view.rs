// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_table_natives::{TableHandle, TableResolver};
use aptos_types::{access_path::AccessPath, state_store::state_key::StateKey};
use bytes::Bytes;
use move_binary_format::errors::{Location, PartialVMError};
use move_core_types::{
    account_address::AccountAddress,
    language_storage::StructTag,
    metadata::Metadata,
    resolver::{resource_size, ResourceResolver},
    value::{IdentifierMappingKind, MoveTypeLayout},
    vm_status::StatusCode,
};
use move_vm_types::{
    value_transformation::{
        deserialize_and_replace_values_with_ids, TransformationError, TransformationResult,
        ValueToIdentifierMapping,
    },
    values::Value,
};
use std::{cell::RefCell, collections::BTreeMap};

/// Models a storage backend which only stores bytes of data.
#[derive(Debug, Default)]
pub struct MockDB {
    db: BTreeMap<StateKey, Bytes>,
}

impl MockDB {
    pub(crate) fn get_bytes(&self, state_key: &StateKey) -> Option<Bytes> {
        self.db.get(state_key).cloned()
    }

    pub(crate) fn store_bytes(&mut self, state_key: StateKey, bytes: Bytes) {
        self.db.insert(state_key, bytes);
    }
}

/// Models a state view which has:
///   1. A map of extracted aggregator / snapshot values.
///   2. A cache layer which models per-block data.
/// . 3. Actual storage backend.
#[derive(Debug, Default)]
pub(crate) struct MockStateView {
    mapping: RefCell<BTreeMap<u64, Value>>,
    in_memory_cache: BTreeMap<StateKey, Bytes>,
    db: MockDB,
}

impl MockStateView {
    pub(crate) fn add_to_db(&mut self, state_key: StateKey, value: Value, layout: MoveTypeLayout) {
        // INVARIANT: All data in storage (base) is stored as is.
        let blob = value
            .simple_serialize(&layout)
            .expect("Deserialization when storing a value always succeeds");
        self.db.store_bytes(state_key, blob.into());
    }

    pub(crate) fn add_mapping(&self, identifier: u64, v: Value) {
        let mut mapping = self.mapping.borrow_mut();
        mapping.insert(identifier, v);
    }

    pub(crate) fn add_to_in_memory_cache(
        &mut self,
        state_key: StateKey,
        value: Value,
        layout: MoveTypeLayout,
    ) {
        // INVARIANT: All data in cache must be lifted.
        // As a result, one should call `add_mapping` before this method.
        let blob = value
            .simple_serialize(&layout)
            .expect("Deserialization when caching a value always succeeds");
        self.in_memory_cache.insert(state_key, blob.into());
    }

    pub(crate) fn assert_mapping_equal_at(&self, identifier: u64, expected_value: Value) {
        assert!(self
            .mapping
            .borrow()
            .get(&identifier)
            .is_some_and(|actual_value| { actual_value.equals(&expected_value).unwrap() }));
    }
}

impl ValueToIdentifierMapping for MockStateView {
    fn value_to_identifier(
        &self,
        _kind: &IdentifierMappingKind,
        layout: &MoveTypeLayout,
        value: Value,
    ) -> TransformationResult<Value> {
        let mut mapping = self.mapping.borrow_mut();
        let identifier = mapping.len() as u64;

        let identifier_value = match layout {
            MoveTypeLayout::U64 => Value::u64(identifier),
            MoveTypeLayout::U128 => Value::u128(identifier as u128),
            _ => {
                return Err(TransformationError::new(format!(
                    "Cannot embed identifier for {}",
                    layout
                )))
            },
        };

        mapping.insert(identifier, value);
        Ok(identifier_value)
    }

    fn identifier_to_value(
        &self,
        layout: &MoveTypeLayout,
        identifier: Value,
    ) -> TransformationResult<Value> {
        let mapping = self.mapping.borrow();
        let identifier = match layout {
            MoveTypeLayout::U64 => identifier.value_as::<u64>().expect(""),
            MoveTypeLayout::U128 => identifier.value_as::<u128>().expect("") as u64,
            _ => {
                return Err(TransformationError::new(format!(
                    "Value {} cannot be an identifier",
                    identifier
                )))
            },
        };

        Ok(mapping
            .get(&identifier)
            .expect("Identifiers must always exist in the mapping")
            .copy_value()
            .expect("Copying mapped values should never fail"))
    }
}

// Performs a serialization round-trip, exchanging values which are supposed
// to be mapped to identifiers.
macro_rules! patch_blob_from_db {
    ($blob:ident, $layout:ident, $exchange:ident) => {
        deserialize_and_replace_values_with_ids(&$blob, $layout, $exchange)
            .map(|value| value.simple_serialize($layout))
            .flatten()
            .ok_or_else(|| {
                PartialVMError::new(StatusCode::VALUE_DESERIALIZATION_ERROR)
                    .with_message("Failed to deserialize and replace with identifiers".to_string())
                    .finish(Location::Undefined)
            })
    };
}

impl ResourceResolver for MockStateView {
    fn get_resource_value_with_metadata(
        &self,
        address: &AccountAddress,
        typ: &StructTag,
        _metadata: &[Metadata],
        layout: &MoveTypeLayout,
    ) -> anyhow::Result<(Option<Bytes>, usize)> {
        let ap = AccessPath::resource_access_path(*address, typ.clone())
            .expect("Access path for resource have to be valid");
        let state_key = StateKey::access_path(ap);

        Ok(match self.in_memory_cache.get(&state_key) {
            Some(blob) => (Some(blob.clone()), blob.len()),
            None => {
                // If a resource is not cached, we must exchange lifted values.
                match self.db.get_bytes(&state_key) {
                    Some(blob) => {
                        let patched_blob = patch_blob_from_db!(blob, layout, self)?;
                        let resource_size = patched_blob.len();
                        (Some(patched_blob.into()), resource_size)
                    },
                    None => (None, 0),
                }
            },
        })
    }

    fn get_resource_bytes_with_metadata(
        &self,
        address: &AccountAddress,
        typ: &StructTag,
        _metadata: &[Metadata],
    ) -> anyhow::Result<(Option<Bytes>, usize)> {
        let ap = AccessPath::resource_access_path(*address, typ.clone())
            .expect("Access path for resource have to be valid");
        let state_key = StateKey::access_path(ap);

        Ok(match self.in_memory_cache.get(&state_key) {
            Some(blob) => (Some(blob.clone()), blob.len()),
            None => {
                let maybe_blob = self.db.get_bytes(&state_key);
                let resource_size = resource_size(&maybe_blob);
                (maybe_blob, resource_size)
            },
        })
    }
}

impl TableResolver for MockStateView {
    fn resolve_table_entry_value(
        &self,
        handle: &TableHandle,
        key: &[u8],
        layout: &MoveTypeLayout,
    ) -> anyhow::Result<Option<Bytes>> {
        let state_key = StateKey::table_item((*handle).into(), key.to_vec());
        Ok(match self.in_memory_cache.get(&state_key) {
            Some(blob) => Some(blob.clone()),
            None => {
                // Otherwise the table entry is not cached and we fetch from storage.
                // Since we have a layout passed, we can need to do the value exchange
                // here by serialization round-trip.
                match self.db.get_bytes(&state_key) {
                    Some(blob) => Some(patch_blob_from_db!(blob, layout, self)?.into()),
                    None => None,
                }
            },
        })
    }

    fn resolve_table_entry_bytes(
        &self,
        handle: &TableHandle,
        key: &[u8],
    ) -> anyhow::Result<Option<Bytes>> {
        let state_key = StateKey::table_item((*handle).into(), key.to_vec());
        Ok(self
            .in_memory_cache
            .get(&state_key)
            .cloned()
            .or_else(|| self.db.get_bytes(&state_key)))
    }
}
