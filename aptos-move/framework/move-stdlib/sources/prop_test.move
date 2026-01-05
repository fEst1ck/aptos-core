#[test_only]
/// Module providing property-based testing functionality. Only included for tests.
module std::prop_test {
    /// Generates a random boolean value uniformly.
    native public fun rand_bool(): bool;

    /// Generates a random u8 value uniformly.
    native public fun rand_u8(): u8;

    /// Generates a random u16 value uniformly.
    native public fun rand_u16(): u16;

    /// Generates a random u32 value uniformly.
    native public fun rand_u32(): u32;

    /// Generates a random u64 value uniformly.
    native public fun rand_u64(): u64;

    /// Generates a random u128 value uniformly.
    native public fun rand_u128(): u128;

    /// Generates a random u256 value uniformly.
    native public fun rand_u256(): u256;

    /// Generates a random address value uniformly.
    native public fun rand_address(): address;

    /// Generates a random u8 value in the range [min_incl, max_excl) (inclusive min, exclusive max).
    /// Aborts if min_incl >= max_excl.
    public fun rand_range_u8(min_incl: u8, max_excl: u8): u8 {
        assert!(min_incl < max_excl, 1);
        let range_size = max_excl - min_incl;
        min_incl + (rand_u8() % range_size)
    }

    /// Generates a random u16 value in the range [min_incl, max_excl) (inclusive min, exclusive max).
    /// Aborts if min_incl >= max_excl.
    public fun rand_range_u16(min_incl: u16, max_excl: u16): u16 {
        assert!(min_incl < max_excl, 1);
        let range_size = max_excl - min_incl;
        min_incl + (rand_u16() % range_size)
    }

    /// Generates a random u32 value in the range [min_incl, max_excl) (inclusive min, exclusive max).
    /// Aborts if min_incl >= max_excl.
    public fun rand_range_u32(min_incl: u32, max_excl: u32): u32 {
        assert!(min_incl < max_excl, 1);
        let range_size = max_excl - min_incl;
        min_incl + (rand_u32() % range_size)
    }

    /// Generates a random u64 value in the range [min_incl, max_excl) (inclusive min, exclusive max).
    /// Aborts if min_incl >= max_excl.
    public fun rand_range_u64(min_incl: u64, max_excl: u64): u64 {
        assert!(min_incl < max_excl, 1);
        let range_size = max_excl - min_incl;
        min_incl + (rand_u64() % range_size)
    }

    /// Generates a random u128 value in the range [min_incl, max_excl) (inclusive min, exclusive max).
    /// Aborts if min_incl >= max_excl.
    public fun rand_range_u128(min_incl: u128, max_excl: u128): u128 {
        assert!(min_incl < max_excl, 1);
        let range_size = max_excl - min_incl;
        min_incl + (rand_u128() % range_size)
    }

    /// Generates a random u256 value in the range [min_incl, max_excl) (inclusive min, exclusive max).
    /// Aborts if min_incl >= max_excl.
    public fun rand_range_u256(min_incl: u256, max_excl: u256): u256 {
        assert!(min_incl < max_excl, 1);
        let range_size = max_excl - min_incl;
        min_incl + (rand_u256() % range_size)
    }
}
