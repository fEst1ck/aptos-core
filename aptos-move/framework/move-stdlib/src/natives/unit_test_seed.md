# Deterministic Random Seed for `unit_test::any<T>()`

The `unit_test::any<T>()` function now supports deterministic random value generation through a configurable seed.

## How It Works

The function uses a seeded RNG (`StdRng`) instead of system entropy (`OsRng`), allowing for reproducible test runs. The seed can be configured in three ways (in priority order):

1. **Programmatically** - via `set_random_seed()`
2. **Environment Variable** - via `MOVE_UNIT_TEST_RANDOM_SEED`
3. **Default** - all zeros (for deterministic behavior)

## Setting the Seed

### Option 1: Programmatically (Recommended for Rust Tests)

```rust
use aptos_move_stdlib::natives::unit_test::set_random_seed;

#[test]
fn my_test() {
    // Set a specific seed before running tests
    set_random_seed([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                     17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]);
    
    // Now run your Move unit tests - they will be deterministic
    run_move_unit_tests(...);
}
```

### Option 2: Environment Variable

```bash
# Set the seed as a 64-character hex string (32 bytes)
export MOVE_UNIT_TEST_RANDOM_SEED="0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

# Or with 0x prefix
export MOVE_UNIT_TEST_RANDOM_SEED="0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"

# Run tests
aptos move test
```

### Option 3: Default (All Zeros)

If no seed is set, the function defaults to `[0u8; 32]` (all zeros), which provides deterministic but predictable values. This is useful for basic testing but may not be ideal for property-based testing.

## Example Usage

```move
module test::example {
    use std::unit_test;

    #[test]
    fun test_deterministic_random() {
        // These values will be the same across test runs with the same seed
        let val1 = unit_test::any<u64>();
        let val2 = unit_test::any<u64>();
        let val3 = unit_test::any<bool>();
        
        // With the same seed, val1, val2, val3 will always be the same
    }
}
```

## Implementation Details

- The seed is stored in a static `Mutex<Option<[u8; 32]>>` for thread-safe access
- The RNG (`StdRng`) is stored in `UnitTestRandomContext` which is added to VM extensions
- Each call to `any<T>()` advances the RNG state, so values are deterministic but different
- The context is created once per test session and shared across all `any<T>()` calls

## Where the Seed is Set

The seed is read when `UnitTestRandomContext::new()` is called, which happens in:
- `aptos-move/aptos-vm/src/natives.rs` in the `unit_test_extensions_hook()` function

This hook is called when unit tests are configured via `configure_for_unit_test()`.
