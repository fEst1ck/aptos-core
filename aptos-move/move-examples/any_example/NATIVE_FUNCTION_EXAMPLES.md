# Native Function Implementation Examples

This document shows various patterns for implementing native functions in Move, based on examples from the Aptos codebase.

## Directory Structure

Native functions are organized by module:
- **`aptos-move/framework/move-stdlib/src/natives/`** - Standard library natives (bcs, hash, string, vector, etc.)
- **`aptos-move/framework/src/natives/`** - Aptos framework natives (account, event, type_info, etc.)

## Common Patterns

### 1. Simple Function with Arguments (No Type Parameters)

**Example: `signer::borrow_address`**
```rust
// aptos-move/framework/move-stdlib/src/natives/signer.rs
fn native_borrow_address(
    context: &mut SafeNativeContext,
    _ty_args: Vec<Type>,  // Empty - no type parameters
    mut arguments: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(_ty_args.is_empty());
    debug_assert!(arguments.len() == 1);

    let signer_reference = safely_pop_arg!(arguments, SignerRef);
    context.charge(SIGNER_BORROW_ADDRESS_BASE)?;
    
    Ok(smallvec![signer_reference.borrow_signer()?])
}
```

### 2. Function with Type Parameters

**Example: `bcs::to_bytes<T>`**
```rust
// aptos-move/framework/move-stdlib/src/natives/bcs.rs
fn native_to_bytes(
    context: &mut SafeNativeContext,
    mut ty_args: Vec<Type>,  // One type parameter T
    mut args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.len() == 1);
    debug_assert!(args.len() == 1);

    let ref_to_val = safely_pop_arg!(args, Reference);
    let arg_type = ty_args.pop().unwrap();
    
    let layout = context.type_to_type_layout(&arg_type)?;
    let val = ref_to_val.read_ref()?;
    
    // ... serialize logic ...
    
    Ok(smallvec![Value::vector_u8(serialized_value)])
}
```

### 3. Function with Gas Metering

**Example: `hash::sha2_256`**
```rust
// aptos-move/framework/move-stdlib/src/natives/hash.rs
fn native_sha2_256(
    context: &mut SafeNativeContext,
    _ty_args: Vec<Type>,
    mut arguments: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    let hash_arg = safely_pop_arg!(arguments, Vec<u8>);
    
    // Charge gas based on input size
    context.charge(
        HASH_SHA2_256_BASE 
        + HASH_SHA2_256_PER_BYTE * NumBytes::new(hash_arg.len() as u64),
    )?;
    
    let hash_vec = Sha256::digest(hash_arg.as_slice()).to_vec();
    Ok(smallvec![Value::vector_u8(hash_vec)])
}
```

### 4. Function with Error Handling

**Example: `util::from_bytes<T>`**
```rust
// aptos-move/framework/src/natives/util.rs
fn native_from_bytes(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    mut args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    let layout = context.type_to_type_layout(&ty_args[0])?;
    let bytes = safely_pop_arg!(args, Vec<u8>);
    
    context.charge(
        UTIL_FROM_BYTES_BASE 
        + UTIL_FROM_BYTES_PER_BYTE * NumBytes::new(bytes.len() as u64),
    )?;
    
    let val = match ValueSerDeContext::new(max_value_nest_depth)
        .deserialize(&bytes, &layout)
    {
        Some(val) => val,
        None => {
            return Err(SafeNativeError::Abort {
                abort_code: EFROM_BYTES,
            })
        },
    };
    
    Ok(smallvec![val])
}
```

### 5. Function with Type Checking

**Example: `type_info::type_of<T>`**
```rust
// aptos-move/framework/src/natives/type_info.rs
fn native_type_of(
    context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    arguments: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.len() == 1);
    debug_assert!(arguments.is_empty());
    
    context.charge(TYPE_INFO_TYPE_OF_BASE)?;
    let type_tag = context.type_to_type_tag(&ty_args[0])?;
    
    if let TypeTag::Struct(struct_tag) = type_tag {
        Ok(type_of_internal(&struct_tag).expect("type_of should never fail."))
    } else {
        Err(SafeNativeError::Abort {
            abort_code: super::status::NFE_EXPECTED_STRUCT_TYPE_TAG,
        })
    }
}
```

### 6. Test-Only Function (Like `any<T>()`)

**Example: `unit_test::any<T>`**
```rust
// aptos-move/framework/move-stdlib/src/natives/unit_test.rs
#[cfg(feature = "testing")]
fn native_any(
    _context: &mut SafeNativeContext,
    ty_args: Vec<Type>,
    args: VecDeque<Value>,
) -> SafeNativeResult<SmallVec<[Value; 1]>> {
    debug_assert!(ty_args.len() == 1);
    debug_assert!(args.is_empty());
    
    let ty = &ty_args[0];
    let mut rng = OsRng;
    
    let value = match ty {
        Type::Bool => Value::bool(rng.next_u32() % 2 == 0),
        Type::U8 => Value::u8(rng.next_u32() as u8),
        // ... other primitives ...
        _ => {
            return Err(SafeNativeError::Abort {
                abort_code: 1,
            });
        },
    };
    
    Ok(smallvec![value])
}
```

## Key Components

### Function Signature
```rust
fn native_function_name(
    context: &mut SafeNativeContext,  // Access to VM context, gas, etc.
    ty_args: Vec<Type>,                // Type arguments (for generics)
    mut args: VecDeque<Value>,         // Function arguments
) -> SafeNativeResult<SmallVec<[Value; 1]>>  // Return values
```

### Common Operations

1. **Extract Arguments:**
   ```rust
   use aptos_native_interface::safely_pop_arg;
   let arg1 = safely_pop_arg!(args, Vec<u8>);
   let arg2 = safely_pop_arg!(args, u64);
   ```

2. **Charge Gas:**
   ```rust
   context.charge(GAS_PARAM_BASE)?;
   context.charge(GAS_PARAM_PER_BYTE * NumBytes::new(bytes.len() as u64))?;
   ```

3. **Create Values:**
   ```rust
   Value::bool(true)
   Value::u64(42)
   Value::vector_u8(vec![1, 2, 3])
   Value::address(AccountAddress::new([0u8; 32]))
   ```

4. **Return Values:**
   ```rust
   use smallvec::{smallvec, SmallVec};
   Ok(smallvec![Value::bool(true)])
   Ok(smallvec![Value::u64(42), Value::u8(1)])  // Multiple return values
   ```

5. **Error Handling:**
   ```rust
   Err(SafeNativeError::Abort {
       abort_code: ERROR_CODE,
   })
   ```

### Registration

All natives must be registered in the `make_all` function:

```rust
pub fn make_all(
    builder: &SafeNativeBuilder,
) -> impl Iterator<Item = (String, NativeFunction)> + '_ {
    let natives = [
        ("function_name", native_function_name as RawSafeNative),
        ("another_function", native_another_function),
    ];
    builder.make_named_natives(natives)
}
```

## Useful Files to Study

1. **Simple natives:** `signer.rs`, `hash.rs`
2. **Generic natives:** `bcs.rs`, `util.rs`
3. **Type manipulation:** `type_info.rs`
4. **Test natives:** `unit_test.rs`
5. **Complex natives:** `event.rs`, `object.rs`

## Tips

- Always use `debug_assert!` to verify argument counts
- Charge gas BEFORE doing expensive operations
- Use `safely_pop_arg!` macro for argument extraction
- Return `SmallVec<[Value; 1]>` for single return values
- Use `context.type_to_type_tag()` or `context.type_to_type_layout()` for type operations
- For test-only functions, use `#[cfg(feature = "testing")]`
