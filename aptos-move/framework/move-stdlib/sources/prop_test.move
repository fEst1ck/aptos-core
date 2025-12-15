#[test_only]
/// Module providing property-based testing functionality. Only included for tests.
module std::prop_test {
    /// Generates a random boolean value.
    native public fun rand_bool(): bool;

    /// Generates a random u8 value.
    native public fun rand_u8(): u8;

    /// Generates a random u16 value.
    native public fun rand_u16(): u16;

    /// Generates a random u32 value.
    native public fun rand_u32(): u32;

    /// Generates a random u64 value.
    native public fun rand_u64(): u64;

    /// Generates a random u128 value.
    native public fun rand_u128(): u128;

    /// Generates a random u256 value.
    native public fun rand_u256(): u256;

    /// Generates a random address value.
    native public fun rand_address(): address;
}
