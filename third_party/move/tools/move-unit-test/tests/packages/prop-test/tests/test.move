module 0x42::test {

    struct S {
        a: address,
        b: u8,
    }

    #[test(...)]
    fun gen_S(): S {
        ..
    }

    #[test(a = range(1, 7))]
    fun test_2(a: u8) {

    }

    #[test(s = gen_S())]
    fun test_1(s : S) {
        ...
    }

    #[test(a = *, b = *)]
    #[proptest(repeat = 100)]
	fun test_0(a: u8, b: u8) {
        assert!(a * b == b * a, 0);
	}
}
