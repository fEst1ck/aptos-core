// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::bounded_math::code_invariant_error;
use aptos_types::state_store::{state_key::StateKey, table::TableHandle};
use move_binary_format::errors::{PartialVMError, PartialVMResult};
use move_core_types::{
    account_address::AccountAddress,
    value::{IdentifierMappingKind, MoveTypeLayout},
    vm_status::StatusCode,
};
use move_vm_types::values::{Struct, Value};

/// Types which implement this trait can be converted to a Move value.
pub trait TryIntoMoveValue: Sized {
    type Error: std::fmt::Display;

    fn try_into_move_value(self, layout: &MoveTypeLayout) -> Result<Value, Self::Error>;
}

/// Types which implement this trait can be constructed from a Move value.
pub trait TryFromMoveValue: Sized {
    // Allows to pass extra information from the caller.
    type Hint;
    type Error: std::fmt::Display;

    fn try_from_move_value(
        layout: &MoveTypeLayout,
        value: Value,
        hint: Option<&Self::Hint>,
    ) -> Result<Self, Self::Error>;
}

pub type AggregatorResult<T> = Result<T, AggregatorError>;

// TODO: Use this instead of PartialVM errors.
#[derive(Debug)]
pub enum AggregatorError {
    WrongVersionID,
}

/// Ephemeral identifier type used by aggregators V2.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct AggregatorID(u64);

impl AggregatorID {
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

// Used for ID generation from u32/u64 counters.
impl From<u64> for AggregatorID {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}

// Returns true if the type layout corresponds to a String, which should be a
// struct with a single byte vector field.
fn is_string_layout<'a>(layout: &'a MoveTypeLayout) -> bool {
    use MoveTypeLayout::*;

    let single_field_struct_fields = |l: &'a MoveTypeLayout| -> Option<&'a [MoveTypeLayout]> {
        match l {
            Struct(struct_layout) if struct_layout.fields().len() == 1 => {
                Some(struct_layout.fields())
            },
            _ => None,
        }
    };

    let is_vec_u8 =
        |l: &MoveTypeLayout| -> bool { matches!(l, Vector(b) if matches!(b.as_ref(), U8)) };

    single_field_struct_fields(layout)
        .map(|ls| is_vec_u8(ls.get(0).expect("A single field must exist")))
        .unwrap_or(false)
}

pub(crate) fn string_from_bytes(bytes: Vec<u8>) -> Value {
    Value::struct_(Struct::pack(vec![Value::vector_u8(bytes)]))
}

fn bytes_from_string(value: Value) -> PartialVMResult<Vec<u8>> {
    value
        .value_as::<Struct>()?
        .unpack()?
        .next()
        .ok_or_else(|| {
            // This should never happen, but the check is still here
            // in case layout and value diverge.
            PartialVMError::new(StatusCode::INDEX_OUT_OF_BOUNDS)
        })?
        .value_as::<Vec<u8>>()
}

impl TryIntoMoveValue for AggregatorID {
    type Error = PartialVMError;

    fn try_into_move_value(self, layout: &MoveTypeLayout) -> Result<Value, Self::Error> {
        Ok(match layout {
            MoveTypeLayout::U64 => Value::u64(self.0),
            MoveTypeLayout::U128 => Value::u128(self.0 as u128),
            layout if is_string_layout(layout) => {
                // WARNING: when changing this mapping, one has to update it
                // for the reverse case.
                string_from_bytes(self.0.to_le_bytes().to_vec())
            },
            // We use ID to value conversion in deserialization.
            _ => {
                return Err(PartialVMError::new(StatusCode::VALUE_DESERIALIZATION_ERROR)
                    .with_message(format!(
                        "Failed to convert {:?} into a Move value with {} layout",
                        self, layout
                    )))
            },
        })
    }
}

impl TryFromMoveValue for AggregatorID {
    type Error = PartialVMError;
    type Hint = ();

    fn try_from_move_value(
        layout: &MoveTypeLayout,
        value: Value,
        _maybe_hint: Option<&Self::Hint>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(match layout {
            MoveTypeLayout::U64 => value.value_as::<u64>()?,
            // SAFETY: We can downcast because IDs are contructed from
            //         values <= u64::MAX.
            MoveTypeLayout::U128 => value.value_as::<u128>()? as u64,
            layout if is_string_layout(layout) => {
                let bytes = bytes_from_string(value)?;
                // SAFETY: We can convert to u64 because IDs which are
                //         contructed from bytes use `u64::to_le_bytes`.
                u64::from_le_bytes(
                    bytes
                        .as_slice()
                        .try_into()
                        .expect("Should be able to construct u64 from bytes"),
                )
            },
            // We use value to ID conversion in serialization.
            _ => {
                return Err(PartialVMError::new(StatusCode::VALUE_SERIALIZATION_ERROR)
                    .with_message(format!(
                        "Failed to convert a Move value with {} layout into an identifier",
                        layout
                    )))
            },
        }))
    }
}

/// Uniquely identifies aggregator or aggregator snapshot instances in
/// extension and possibly storage.
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum AggregatorVersionedID {
    // Aggregator V1 is implemented as a state item, and so can be queried by
    // the state key.
    V1(StateKey),
    // Aggregator V2 is embedded into resources, and uses ephemeral identifiers
    // which are unique per block.
    V2(AggregatorID),
}

impl AggregatorVersionedID {
    pub fn v1(handle: TableHandle, key: AccountAddress) -> Self {
        let state_key = StateKey::table_item(handle, key.to_vec());
        Self::V1(state_key)
    }

    pub fn v2(value: u64) -> Self {
        Self::V2(AggregatorID::new(value))
    }
}

impl TryFrom<AggregatorVersionedID> for StateKey {
    type Error = AggregatorError;

    fn try_from(vid: AggregatorVersionedID) -> Result<Self, Self::Error> {
        match vid {
            AggregatorVersionedID::V1(state_key) => Ok(state_key),
            AggregatorVersionedID::V2(_) => Err(AggregatorError::WrongVersionID),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AggregatorValue {
    Aggregator(u128),
    Snapshot(u128),
    Derived(Vec<u8>),
}

impl AggregatorValue {
    pub fn into_aggregator_value(self) -> PartialVMResult<u128> {
        match self {
            AggregatorValue::Aggregator(value) => Ok(value),
            AggregatorValue::Snapshot(_) => Err(code_invariant_error(
                "Tried calling into_aggregator_value on Snapshot value",
            )),
            AggregatorValue::Derived(_) => Err(code_invariant_error(
                "Tried calling into_aggregator_value on String SnapshotValue",
            )),
        }
    }

    pub fn into_snapshot_value(self) -> PartialVMResult<u128> {
        match self {
            AggregatorValue::Snapshot(value) => Ok(value),
            AggregatorValue::Aggregator(_) => Err(code_invariant_error(
                "Tried calling into_snapshot_value on Aggregator value",
            )),
            AggregatorValue::Derived(_) => Err(code_invariant_error(
                "Tried calling into_snapshot_value on String SnapshotValue",
            )),
        }
    }

    pub fn into_derived_value(self) -> PartialVMResult<Vec<u8>> {
        match self {
            AggregatorValue::Derived(value) => Ok(value),
            AggregatorValue::Aggregator(_) => Err(code_invariant_error(
                "Tried calling into_derived_value on Aggregator value",
            )),
            AggregatorValue::Snapshot(_) => Err(code_invariant_error(
                "Tried calling into_derived_value on Snapshot value",
            )),
        }
    }
}

impl TryIntoMoveValue for AggregatorValue {
    type Error = PartialVMError;

    fn try_into_move_value(self, layout: &MoveTypeLayout) -> Result<Value, Self::Error> {
        use AggregatorValue::*;
        use MoveTypeLayout::*;

        Ok(match (self, layout) {
            (Aggregator(v) | Snapshot(v), U64) => Value::u64(v as u64),
            (Aggregator(v) | Snapshot(v), U128) => Value::u128(v),
            (Derived(bytes), layout) if is_string_layout(layout) => string_from_bytes(bytes),
            (value, layout) => {
                return Err(PartialVMError::new(StatusCode::VALUE_SERIALIZATION_ERROR)
                    .with_message(format!(
                        "Failed to convert {:?} into Move value with {} layout",
                        value, layout
                    )))
            },
        })
    }
}

impl TryFromMoveValue for AggregatorValue {
    type Error = PartialVMError;
    // Need to distinguish between aggregators and snapshots of integer types.
    // TODO: We only need that because of the current enum-based implementations.
    type Hint = IdentifierMappingKind;

    fn try_from_move_value(
        layout: &MoveTypeLayout,
        value: Value,
        maybe_hint: Option<&Self::Hint>,
    ) -> Result<Self, Self::Error> {
        let kind = maybe_hint.expect("Hint should be provided to construct AggregatorValue");

        use AggregatorValue::*;
        use IdentifierMappingKind as K;
        use MoveTypeLayout::*;

        Ok(match (kind, layout) {
            (K::Aggregator, U64) => Aggregator(value.value_as::<u64>()? as u128),
            (K::Aggregator, U128) => Aggregator(value.value_as::<u128>()?),
            (K::Snapshot, U64) => Snapshot(value.value_as::<u64>()? as u128),
            (K::Snapshot, U128) => Snapshot(value.value_as::<u128>()?),
            (K::Snapshot, layout) if is_string_layout(layout) => {
                let bytes = bytes_from_string(value)?;
                Derived(bytes)
            },
            _ => {
                return Err(PartialVMError::new(StatusCode::VALUE_DESERIALIZATION_ERROR)
                    .with_message(format!(
                        "Failed to convert Move value {:?} with {} layout into AggregatorValue",
                        value, layout
                    )))
            },
        })
    }
}

// TODO see if we need both AggregatorValue and SnapshotValue. Also, maybe they should be nested
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotValue {
    Integer(u128),
    String(Vec<u8>),
}

impl SnapshotValue {
    pub fn into_aggregator_value(self) -> PartialVMResult<u128> {
        match self {
            SnapshotValue::Integer(value) => Ok(value),
            SnapshotValue::String(_) => Err(code_invariant_error(
                "Tried calling into_aggregator_value on String SnapshotValue",
            )),
        }
    }
}

impl TryFrom<AggregatorValue> for SnapshotValue {
    type Error = PartialVMError;

    fn try_from(value: AggregatorValue) -> PartialVMResult<SnapshotValue> {
        match value {
            AggregatorValue::Aggregator(_) => Err(code_invariant_error(
                "Tried calling SnapshotValue::try_from on AggregatorValue(Aggregator)",
            )),
            AggregatorValue::Snapshot(v) => Ok(SnapshotValue::Integer(v)),
            AggregatorValue::Derived(v) => Ok(SnapshotValue::String(v)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotToStringFormula {
    Concat { prefix: Vec<u8>, suffix: Vec<u8> },
}

impl SnapshotToStringFormula {
    pub fn apply(&self, base: u128) -> Vec<u8> {
        match self {
            SnapshotToStringFormula::Concat { prefix, suffix } => {
                let mut result = prefix.clone();
                result.extend(base.to_string().as_bytes());
                result.extend(suffix);
                result
            },
        }
    }
}
