// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

// Copyright (c) The Diem Core Contributors
// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use aptos_native_interface::{
    safely_pop_arg, RawSafeNative, SafeNativeBuilder, SafeNativeContext, SafeNativeError,
    SafeNativeResult,
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

/// Context for deterministic random value generation in unit tests.
#[derive(Tid)]
pub struct UnitTestRandomContext {
    rng: StdRng,
}

impl UnitTestRandomContext {
    /// Creates a new context with a seed. The seed can be set via the `MOVE_UNIT_TEST_RANDOM_SEED`
    /// environment variable (as a hex string), or defaults to all zeros for deterministic behavior.
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

    fn next_u32(&mut self) -> u32 {
        self.rng.next_u32()
    }

    fn next_u64(&mut self) -> u64 {
        self.rng.next_u64()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
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
 * native fun create_signers_for_testing
 *
 *   gas cost: base_cost + unit_cost * num_of_signers
 *
 **************************************************************************************************/
fn to_le_bytes(i: u64) -> [u8; AccountAddress::LENGTH] {
    let bytes = i.to_le_bytes();
    let mut result = [0u8; AccountAddress::LENGTH];
    result[..bytes.len()].clone_from_slice(bytes.as_ref());
    result
}

fn native_create_signers_for_testing(
    _context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    mut args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.is_empty());
    debug_assert!(args.len() == 1);

    let num_signers = safely_pop_arg!(args, u64);

    let signers = Value::vector_for_testing_only(
        (0..num_signers).map(|i| Value::master_signer(AccountAddress::new(to_le_bytes(i)))),
    );

    Ok(smallvec![signers])
}

/***************************************************************************************************
 * native fun any
 *
 *   Generates a random value of a primitive type T.
 *   Supported types: bool, u8, u16, u32, u64, u128, u256, address
 *
 *   gas cost: no
 *
 **************************************************************************************************/
fn native_any(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.len() == 1);
    debug_assert!(args.is_empty());

    let ty = &ty_args[0];
    
    // Get the deterministic RNG from extensions
    let random_ctx = context.extensions_mut().get_mut::<UnitTestRandomContext>();

    let value = match ty {
        Type::Bool => Value::bool(random_ctx.next_u32() % 2 == 0),
        Type::U8 => Value::u8(random_ctx.next_u32() as u8),
        Type::U16 => Value::u16(random_ctx.next_u32() as u16),
        Type::U32 => Value::u32(random_ctx.next_u32()),
        Type::U64 => Value::u64(random_ctx.next_u64()),
        Type::U128 => {
            let high = random_ctx.next_u64() as u128;
            let low = random_ctx.next_u64() as u128;
            Value::u128((high << 64) | low)
        },
        Type::U256 => {
            let mut bytes = [0u8; 32];
            random_ctx.fill_bytes(&mut bytes);
            Value::u256(U256::from_le_bytes(&bytes))
        },
        Type::Address => {
            let mut bytes = [0u8; AccountAddress::LENGTH];
            random_ctx.fill_bytes(&mut bytes);
            Value::address(AccountAddress::new(bytes))
        },
        _ => {
            return Err(SafeNativeError::Abort {
                abort_code: 1,
            });
        },
    };

    Ok(smallvec![value])
}

/***************************************************************************************************
 * module
 **************************************************************************************************/
pub fn make_all(
    builder: &SafeNativeBuilder,
) -> impl Iterator<Item = (String, NativeFunction)> + '_ {
    let natives = [
        (
            "create_signers_for_testing",
            native_create_signers_for_testing as RawSafeNative,
        ),
        ("any", native_any as RawSafeNative),
    ];

    builder.make_named_natives(natives)
}
