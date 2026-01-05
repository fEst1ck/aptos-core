/// Minimal example module demonstrating the use of prop_test random functions
module any_example::example {
    #[test_only]
    use std::prop_test;
    #[test_only]
    use std::prop_test::rand_bool;
    #[test_only]
    use aptos_std::debug;
    use std::vector;

    #[test]
    fun test_rand_primitives() {
        // Generate random values for different primitive types
        let random_bool = prop_test::rand_bool();
        debug::print(&random_bool);
        
        let random_u8 = prop_test::rand_u8();
        debug::print(&random_u8);
        
        let random_u16 = prop_test::rand_u16();
        debug::print(&random_u16);
        
        let random_u32 = prop_test::rand_u32();
        debug::print(&random_u32);
        
        let random_u64 = prop_test::rand_u64();
        debug::print(&random_u64);
        
        let random_u128 = prop_test::rand_u128();
        debug::print(&random_u128);
        
        let random_u256 = prop_test::rand_u256();
        debug::print(&random_u256);
        
        let random_address = prop_test::rand_address();
        debug::print(&random_address);
    }

    #[test]
    fun test_rand_in_conditionals() {
        // Use random values in conditional logic
        let random_bool = prop_test::rand_bool();
        debug::print(&random_bool);
        if (random_bool) {
            let random_u64 = prop_test::rand_u64();
            debug::print(&random_u64);
            assert!(random_u64 >= 0, 1);
        } else {
            let random_address = prop_test::rand_address();
            debug::print(&random_address);
            assert!(random_address != @0x0, 2);
        };
    }

    #[test]
    fun test_rand_for_comparison() {
        // Generate two random values and compare them
        let value1 = prop_test::rand_u64();
        debug::print(&value1);
        let value2 = prop_test::rand_u64();
        debug::print(&value2);
        
        // They might be equal or different (both are valid)
        assert!(value1 == value1, 3);
        assert!(value2 == value2, 4);
    }

    #[test]
    fun test1() {
        for (i in 0..10) {
            debug::print(&prop_test::rand_u8());
        };
    }

    #[test]
    fun test2() {
        debug::print(&prop_test::rand_u64());
        debug::print(&prop_test::rand_u64());
        debug::print(&prop_test::rand_u64());
    }

    #[test]
    fun test_rand_range() {
        // Test range functions [min_incl, max_excl)
        let u8_val = prop_test::rand_range_u8(10, 11);
        debug::print(&u8_val);
        assert!(u8_val >= 10 && u8_val < 20, 1);

        let u64_val = prop_test::rand_range_u64(100, 200);
        debug::print(&u64_val);
        assert!(u64_val >= 100 && u64_val < 200, 2);
    }

    #[test_only]
    fun gen_u8(): u8 {  
        prop_test::rand_u8()
    }

    // Generator function that creates a random u64 value
    #[test_only]
    fun gen_u64(): u64 {
        prop_test::rand_u64()
    }

    // Generator function that creates a random address
    #[test_only]
    fun gen_address(): address {
        prop_test::rand_address()
    }

    // Example using function call generator - the generator function is called before the test
    #[test(val = gen_u64)]
    #[proptest(repeat = 5)]
    fun test_with_generator(val: u64) {
        debug::print(&val);
        assert!(val >= 0, 3);
    }

    // Example using multiple generators
    #[test(addr = gen_address, num = gen_u8)]
    fun test_multiple_generators(addr: address, num: u8) {
        debug::print(&addr);
        debug::print(&num);
        assert!(addr != @0x0 || num > 0, 4);
    }

    struct S has drop {
        x: u8,
        y: bool,
    }

    // fun gen_one_of(x: vector<u8>): u8 {
    //     let index = prop_test::rand_range_u8(0, x.length());
    //     x[index]
    // }

    // fun gen_counter(): || -> u8 {
    //     let counter = 0;
    //     return || {
    //         counter = counter + 1;
    //         counter
    //     }
    // }

    #[test_only]
    fun gen_S(x: u8): S {
        S { x: x, y: prop_test::rand_bool() }
    }

    // #[test(s = gen_u8)]
    fun test_struct_generator(s: S) {
        debug::print(&s);
    }

    #[test]
    fun stateful() {
        let x = gen_S(1);
        debug::print(&x);
        let y = gen_S(2);
        debug::print(&y);
    }

    #[test(b = rand_bool)]
    fun test_rand_bool(b: bool) {
        debug::print(&b);
    }

    #[test_only]
    fun random_vec_u8(len: u8): vector<u8> {
        let vec = vector::empty<u8>();
        for (i in 0..len) {
            vector::push_back(&mut vec, prop_test::rand_u8());
        };
        vec 
    }

    // #[test]
    // fun test_random_vec() {
    //     let vec = random_vec<u8>(10);
    //     debug::print(&vec[0]);
    //     debug::print(&vec[1]);
    //     debug::print(&vec[2]);
    //     debug::print(&vec[3]);
    //     debug::print(&vec[4]);
    //     debug::print(&vec[5]);
    //     debug::print(&vec[6]);
    //     debug::print(&vec[7]);
    //     debug::print(&vec[8]);
    //     debug::print(&vec[9]);
    // }

    #[test_only]
    fun test_logic(x: u8) {

    }

    #[test]
    fun test_concrete() {
        test_logic(1);
        test_logic(2);
        test_logic(3);
    }

    
}
