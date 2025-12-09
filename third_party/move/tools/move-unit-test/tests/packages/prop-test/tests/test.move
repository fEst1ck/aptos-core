module 0x42::test {
    #[test(a = *, b = *)]
	fun test_0(a: u8, b: u8) {
        assert!(a * b == b * a, 0);
	}
}