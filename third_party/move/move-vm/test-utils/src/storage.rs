// Copyright (c) The Diem Core Contributors
// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use anyhow::{bail, Error, Result};
use bytes::Bytes;
use move_core_types::{
    account_address::AccountAddress,
    effects::{AccountChangeSet, ChangeSet, Op},
    identifier::Identifier,
    language_storage::{ModuleId, StructTag},
    metadata::Metadata,
    resolver::{ModuleResolver, MoveResolver, ResourceResolver},
    value::{MoveTypeLayout, BytesWithLayout},
};
#[cfg(feature = "table-extension")]
use move_table_extension::{TableChangeSet, TableHandle, TableResolver};
use std::{
    collections::{btree_map, BTreeMap},
    fmt::Debug,
};

/// A dummy storage containing no modules or resources.
#[derive(Debug, Clone)]
pub struct BlankStorage;

impl BlankStorage {
    pub fn new() -> Self {
        Self
    }
}

impl ModuleResolver for BlankStorage {
    fn get_module_metadata(&self, _module_id: &ModuleId) -> Vec<Metadata> {
        vec![]
    }

    fn get_module(&self, _module_id: &ModuleId) -> Result<Option<Bytes>> {
        Ok(None)
    }
}

impl ResourceResolver for BlankStorage {
    fn get_resource_value_with_metadata(
        &self,
        _address: &AccountAddress,
        _tag: &StructTag,
        _metadata: &[Metadata],
        _layout: &MoveTypeLayout,
    ) -> Result<(Option<Bytes>, usize)> {
        Ok((None, 0))
    }

    fn get_resource_bytes_with_metadata(
        &self,
        _address: &AccountAddress,
        _tag: &StructTag,
        _metadata: &[Metadata],
    ) -> Result<(Option<Bytes>, usize)> {
        Ok((None, 0))
    }
}

#[cfg(feature = "table-extension")]
impl TableResolver for BlankStorage {
    fn resolve_table_entry_value(
        &self,
        _handle: &TableHandle,
        _key: &[u8],
        _layout: &MoveTypeLayout,
    ) -> Result<Option<Bytes>, Error> {
        Ok(None)
    }

    fn resolve_table_entry_bytes(
        &self,
        _handle: &TableHandle,
        _key: &[u8],
    ) -> Result<Option<Bytes>, Error> {
        Ok(None)
    }
}

/// A storage adapter created by stacking a change set on top of an existing storage backend.
/// This can be used for additional computations without modifying the base.
#[derive(Debug, Clone)]
pub struct DeltaStorage<'a, 'b, S> {
    base: &'a S,
    change_set: &'b ChangeSet,
}

impl<'a, 'b, S: ModuleResolver> ModuleResolver for DeltaStorage<'a, 'b, S> {
    fn get_module_metadata(&self, _module_id: &ModuleId) -> Vec<Metadata> {
        vec![]
    }

    fn get_module(&self, module_id: &ModuleId) -> Result<Option<Bytes>, Error> {
        if let Some(account_storage) = self.change_set.accounts().get(module_id.address()) {
            if let Some(blob_opt) = account_storage.modules().get(module_id.name()) {
                return Ok(blob_opt.clone().ok());
            }
        }

        self.base.get_module(module_id)
    }
}

fn get_bytes_and_size(
    buf: Option<BytesWithLayout>,
) -> Result<(Option<Bytes>, usize)> {
    let buf_bytes = buf.as_ref().map(|(bytes, _)| bytes);
    let buf_size = buf_bytes.map(|bytes| bytes.len()).unwrap_or(0);
    Ok((buf_bytes.cloned(), buf_size))
}

impl<'a, 'b, S: ResourceResolver> ResourceResolver for DeltaStorage<'a, 'b, S> {
    fn get_resource_bytes_with_metadata(
        &self,
        address: &AccountAddress,
        tag: &StructTag,
        metadata: &[Metadata],
    ) -> Result<(Option<Bytes>, usize)> {
        if let Some(account_storage) = self.change_set.accounts().get(address) {
            if let Some(blob_opt) = account_storage.resources().get(tag) {
                let buf = blob_opt.clone().ok();
                return get_bytes_and_size(buf);
            }
        }
        self.base
            .get_resource_bytes_with_metadata(address, tag, metadata)
    }
}

#[cfg(feature = "table-extension")]
impl<'a, 'b, S: TableResolver> TableResolver for DeltaStorage<'a, 'b, S> {
    fn resolve_table_entry_value(
        &self,
        handle: &TableHandle,
        key: &[u8],
        layout: &MoveTypeLayout,
    ) -> Result<Option<Bytes>, Error> {
        // TODO: In addition to `change_set`, cache table outputs.
        self.base.resolve_table_entry_value(handle, key, layout)
    }

    fn resolve_table_entry_bytes(
        &self,
        handle: &TableHandle,
        key: &[u8],
    ) -> Result<Option<Bytes>, Error> {
        // TODO: In addition to `change_set`, cache table outputs.
        self.base.resolve_table_entry_bytes(handle, key)
    }
}

impl<'a, 'b, S: MoveResolver> DeltaStorage<'a, 'b, S> {
    pub fn new(base: &'a S, delta: &'b ChangeSet) -> Self {
        Self {
            base,
            change_set: delta,
        }
    }
}

/// Simple in-memory storage for modules and resources under an account.
#[derive(Debug, Clone)]
struct InMemoryAccountStorage {
    resources: BTreeMap<StructTag, BytesWithLayout>,
    modules: BTreeMap<Identifier, Bytes>,
}

/// Simple in-memory storage that can be used as a Move VM storage backend for testing purposes.
#[derive(Debug, Clone)]
pub struct InMemoryStorage {
    accounts: BTreeMap<AccountAddress, InMemoryAccountStorage>,
    #[cfg(feature = "table-extension")]
    tables: BTreeMap<TableHandle, BTreeMap<Vec<u8>, Bytes>>,
}

fn apply_changes<K, V>(
    map: &mut BTreeMap<K, V>,
    changes: impl IntoIterator<Item = (K, Op<V>)>,
) -> Result<()>
where
    K: Ord + Debug,
{
    use btree_map::Entry::*;
    use Op::*;

    for (k, op) in changes.into_iter() {
        match (map.entry(k), op) {
            (Occupied(entry), New(_)) => {
                bail!(
                    "Failed to apply changes -- key {:?} already exists",
                    entry.key()
                )
            },
            (Occupied(entry), Delete) => {
                entry.remove();
            },
            (Occupied(entry), Modify(val)) => {
                *entry.into_mut() = val;
            },
            (Vacant(entry), New(val)) => {
                entry.insert(val);
            },
            (Vacant(entry), Delete | Modify(_)) => bail!(
                "Failed to apply changes -- key {:?} does not exist",
                entry.key()
            ),
        }
    }
    Ok(())
}

fn get_or_insert<K, V, F>(map: &mut BTreeMap<K, V>, key: K, make_val: F) -> &mut V
where
    K: Ord,
    F: FnOnce() -> V,
{
    use btree_map::Entry::*;

    match map.entry(key) {
        Occupied(entry) => entry.into_mut(),
        Vacant(entry) => entry.insert(make_val()),
    }
}

impl InMemoryAccountStorage {
    fn apply(&mut self, account_changeset: AccountChangeSet) -> Result<()> {
        let (modules, resources) = account_changeset.into_inner();
        apply_changes(&mut self.modules, modules)?;
        apply_changes(&mut self.resources, resources)?;
        Ok(())
    }

    fn new() -> Self {
        Self {
            modules: BTreeMap::new(),
            resources: BTreeMap::new(),
        }
    }
}

impl InMemoryStorage {
    pub fn apply_extended(
        &mut self,
        changeset: ChangeSet,
        #[cfg(feature = "table-extension")] table_changes: TableChangeSet,
    ) -> Result<()> {
        for (addr, account_changeset) in changeset.into_inner() {
            match self.accounts.entry(addr) {
                btree_map::Entry::Occupied(entry) => {
                    entry.into_mut().apply(account_changeset)?;
                },
                btree_map::Entry::Vacant(entry) => {
                    let mut account_storage = InMemoryAccountStorage::new();
                    account_storage.apply(account_changeset)?;
                    entry.insert(account_storage);
                },
            }
        }

        #[cfg(feature = "table-extension")]
        self.apply_table(table_changes)?;

        Ok(())
    }

    pub fn apply(&mut self, changeset: ChangeSet) -> Result<()> {
        self.apply_extended(
            changeset,
            #[cfg(feature = "table-extension")]
            TableChangeSet::default(),
        )
    }

    #[cfg(feature = "table-extension")]
    fn apply_table(&mut self, changes: TableChangeSet) -> Result<()> {
        let TableChangeSet {
            new_tables,
            removed_tables,
            changes,
        } = changes;
        self.tables.retain(|h, _| !removed_tables.contains(h));
        self.tables
            .extend(new_tables.keys().map(|h| (*h, BTreeMap::default())));
        for (h, c) in changes {
            assert!(
                self.tables.contains_key(&h),
                "inconsistent table change set: stale table handle"
            );
            let table = self.tables.get_mut(&h).unwrap();
            apply_changes(table, c.entries)?;
        }
        Ok(())
    }

    pub fn new() -> Self {
        Self {
            accounts: BTreeMap::new(),
            #[cfg(feature = "table-extension")]
            tables: BTreeMap::new(),
        }
    }

    pub fn publish_or_overwrite_module(&mut self, module_id: ModuleId, blob: Vec<u8>) {
        let account = get_or_insert(&mut self.accounts, *module_id.address(), || {
            InMemoryAccountStorage::new()
        });
        account
            .modules
            .insert(module_id.name().to_owned(), blob.into());
    }

    pub fn publish_or_overwrite_resource(
        &mut self,
        addr: AccountAddress,
        struct_tag: StructTag,
        blob: Vec<u8>,
    ) {
        let account = get_or_insert(&mut self.accounts, addr, InMemoryAccountStorage::new);
        account.resources.insert(struct_tag, (blob.into(), None));
    }
}

impl ModuleResolver for InMemoryStorage {
    fn get_module_metadata(&self, _module_id: &ModuleId) -> Vec<Metadata> {
        vec![]
    }

    fn get_module(&self, module_id: &ModuleId) -> Result<Option<Bytes>, Error> {
        if let Some(account_storage) = self.accounts.get(module_id.address()) {
            return Ok(account_storage.modules.get(module_id.name()).cloned());
        }
        Ok(None)
    }
}

impl ResourceResolver for InMemoryStorage {
    fn get_resource_bytes_with_metadata(
        &self,
        address: &AccountAddress,
        tag: &StructTag,
        _metadata: &[Metadata],
    ) -> Result<(Option<Bytes>, usize)> {
        if let Some(account_storage) = self.accounts.get(address) {
            let buf = account_storage.resources.get(tag).cloned();
            return get_bytes_and_size(buf);
        }
        Ok((None, 0))
    }
}

#[cfg(feature = "table-extension")]
impl TableResolver for InMemoryStorage {
    fn resolve_table_entry_bytes(
        &self,
        handle: &TableHandle,
        key: &[u8],
    ) -> std::result::Result<Option<Bytes>, Error> {
        Ok(self.tables.get(handle).and_then(|t| t.get(key).cloned()))
    }
}
