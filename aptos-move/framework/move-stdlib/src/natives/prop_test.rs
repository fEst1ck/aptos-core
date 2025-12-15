// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_native_interface::{
    RawSafeNative, SafeNativeBuilder, SafeNativeContext, SafeNativeResult,
};
use better_any::{Tid, TidAble};
use move_core_types::account_address::AccountAddress;
use move_core_types::u256::U256;
use move_vm_runtime::native_functions::NativeFunction;
use move_vm_types::loaded_data::runtime_types::Type;
use move_vm_types::values::Value;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use smallvec::{smallvec, SmallVec};
use std::{
    collections::VecDeque,
    env,
};

/// Context for deterministic random value generation in property-based tests.
#[derive(Tid)]
pub struct UnitTestRandomContext {
    rng: StdRng,
}

impl UnitTestRandomContext {
    /// Creates a new context with a seed. The seed can be set via the `MOVE_UNIT_TEST_RANDOM_SEED`
    /// environment variable (as a decimal number), or defaults to all zeros for deterministic behavior.
    pub fn new() -> Self {
        let seed = get_random_seed();
        Self {
            rng: StdRng::from_seed(seed),
        }
    }

    /// Creates a new context with a specific seed.
    pub fn with_seed(seed: [u8; 32]) -> Self {
        Self {
            rng: StdRng::from_seed(seed),
        }
    }

    pub(crate) fn next_u32(&mut self) -> u32 {
        self.rng.next_u32()
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    pub(crate) fn fill_bytes(&mut self, dest: &mut [u8]) {
        self.rng.fill_bytes(dest);
    }
}

/// Gets the random seed from environment variable or returns default.
/// Environment variable `MOVE_UNIT_TEST_RANDOM_SEED` should be a decimal number (e.g., "42" or "12345").
/// If not set, defaults to all zeros for deterministic behavior.
fn get_random_seed() -> [u8; 32] {
    if let Ok(seed_str) = env::var("MOVE_UNIT_TEST_RANDOM_SEED") {
        if let Ok(num) = seed_str.trim().parse::<u64>() {
            let mut seed = [0u8; 32];
            // Convert u64 to bytes (little-endian) in the first 8 bytes
            seed[..8].copy_from_slice(&num.to_le_bytes());
            // Remaining bytes are already zero
            return seed;
        }
    }
    // Default seed: all zeros for deterministic behavior
    [0u8; 32]
}

/***************************************************************************************************
 * native fun rand_bool
 *
 *   Generates a random boolean value.
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_rand_bool(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.is_empty());

    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();
    Ok(smallvec![Value::bool(random_ctx.next_u32() % 2 == 0)])
}

/***************************************************************************************************
 * native fun rand_u8
 *
 *   Generates a random u8 value.
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_rand_u8(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.is_empty());

    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();
    Ok(smallvec![Value::u8(random_ctx.next_u32() as u8)])
}

/***************************************************************************************************
 * native fun rand_u16
 *
 *   Generates a random u16 value.
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_rand_u16(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.is_empty());

    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();
    Ok(smallvec![Value::u16(random_ctx.next_u32() as u16)])
}

/***************************************************************************************************
 * native fun rand_u32
 *
 *   Generates a random u32 value.
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_rand_u32(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.is_empty());

    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();
    Ok(smallvec![Value::u32(random_ctx.next_u32())])
}

/***************************************************************************************************
 * native fun rand_u64
 *
 *   Generates a random u64 value.
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_rand_u64(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.is_empty());

    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();
    Ok(smallvec![Value::u64(random_ctx.next_u64())])
}

/***************************************************************************************************
 * native fun rand_u128
 *
 *   Generates a random u128 value.
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_rand_u128(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.is_empty());

    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();
    let high = random_ctx.next_u64() as u128;
    let low = random_ctx.next_u64() as u128;
    Ok(smallvec![Value::u128((high << 64) | low)])
}

/***************************************************************************************************
 * native fun rand_u256
 *
 *   Generates a random u256 value.
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_rand_u256(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.is_empty());

    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();
    let mut bytes = [0u8; 32];
    random_ctx.fill_bytes(&mut bytes);
    Ok(smallvec![Value::u256(U256::from_le_bytes(&bytes))])
}

/***************************************************************************************************
 * native fun rand_address
 *
 *   Generates a random address value.
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_rand_address(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.is_empty());

    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();
    let mut bytes = [0u8; AccountAddress::LENGTH];
    random_ctx.fill_bytes(&mut bytes);
    Ok(smallvec![Value::address(AccountAddress::new(bytes))])
}

/***************************************************************************************************
 * module
 **************************************************************************************************/
pub fn make_all(
    builder: &SafeNativeBuilder,
) -> impl Iterator<Item = (String, NativeFunction)> + '_ {
    let natives = [
        ("rand_bool", native_rand_bool as RawSafeNative),
        ("rand_u8", native_rand_u8 as RawSafeNative),
        ("rand_u16", native_rand_u16 as RawSafeNative),
        ("rand_u32", native_rand_u32 as RawSafeNative),
        ("rand_u64", native_rand_u64 as RawSafeNative),
        ("rand_u128", native_rand_u128 as RawSafeNative),
        ("rand_u256", native_rand_u256 as RawSafeNative),
        ("rand_address", native_rand_address as RawSafeNative),
    ];

    builder.make_named_natives(natives)
}
