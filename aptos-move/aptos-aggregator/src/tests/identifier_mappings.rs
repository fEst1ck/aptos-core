// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::types::{AggregatorID, AggregatorValue, TryFromMoveValue, TryIntoMoveValue};
use claims::{assert_err, assert_ok};
use move_core_types::value::{IdentifierMappingKind, MoveStructLayout, MoveTypeLayout};
use move_vm_types::values::Value;

const ID_FOR_TEST: u64 = 100;

macro_rules! assert_aggregator_id {
    ($layout:expr) => {
        let value = assert_ok!(AggregatorID::new(ID_FOR_TEST).try_into_move_value(&$layout));
        let id = assert_ok!(AggregatorID::try_from_move_value(&$layout, value, None));
        assert_eq!(id, AggregatorID::new(ID_FOR_TEST));
    };
}

#[test]
fn test_aggregator_value_conversion() {
    use MoveStructLayout::*;
    use MoveTypeLayout::*;

    // Supported types: u64, u128 and String.
    assert_aggregator_id!(U64);
    assert_aggregator_id!(U128);
    assert_aggregator_id!(Struct(Runtime(vec![Vector(Box::new(U8))])));

    // Cannot convert IDs with arbitrary type layouts.
    assert_err!(AggregatorID::new(ID_FOR_TEST).try_into_move_value(&Bool));
    assert_err!(AggregatorID::new(ID_FOR_TEST).try_into_move_value(&Vector(Box::new(U8))));

    // Cannot construct IDs with wrong type layouts / values.
    assert_err!(AggregatorID::try_from_move_value(
        &U64,
        Value::u128(10),
        None
    ));
    assert_err!(AggregatorID::try_from_move_value(
        &Bool,
        Value::u64(10),
        None
    ));
    assert_err!(AggregatorID::try_from_move_value(
        &U128,
        Value::u8(10),
        None
    ));
}

macro_rules! assert_aggregator_value {
    ($v:expr, $layout:expr, $hint:expr) => {
        let value = assert_ok!($v.clone().try_into_move_value(&$layout));
        let v = assert_ok!(AggregatorValue::try_from_move_value(
            &$layout,
            value,
            Some(&$hint)
        ));
        assert_eq!(v, $v);
    };
}

#[test]
fn test_aggregator_id_conversion() {
    use AggregatorValue::*;
    use IdentifierMappingKind as K;
    use MoveStructLayout::*;
    use MoveTypeLayout::*;

    // Support: u64/u128 aggregators and snapshots, plus derived
    // String values.
    assert_aggregator_value!(Aggregator(10), U64, K::Aggregator);
    assert_aggregator_value!(Aggregator(10), U128, K::Aggregator);
    assert_aggregator_value!(Snapshot(10), U64, K::Snapshot);
    assert_aggregator_value!(Snapshot(10), U128, K::Snapshot);
    assert_aggregator_value!(
        Derived(vec![0, 1, 2]),
        Struct(Runtime(vec![Vector(Box::new(U8))])),
        K::Snapshot
    );

    // Wrong layouts for the chosen value.
    assert_err!(Aggregator(10).try_into_move_value(&Bool));
    assert_err!(Aggregator(10).try_into_move_value(&Struct(Runtime(vec![Vector(Box::new(U8))]))));
    assert_err!(Snapshot(10).try_into_move_value(&Bool));
    assert_err!(Snapshot(10).try_into_move_value(&Struct(Runtime(vec![Vector(Box::new(U8))]))));
    assert_err!(Derived(vec![0, 1, 2]).try_into_move_value(&U64));

    // Wrong layouts / values.
    assert_err!(AggregatorValue::try_from_move_value(
        &U64,
        Value::u128(10),
        Some(&K::Aggregator)
    ));
    assert_err!(AggregatorValue::try_from_move_value(
        &Bool,
        Value::u128(10),
        Some(&K::Aggregator)
    ));
}
