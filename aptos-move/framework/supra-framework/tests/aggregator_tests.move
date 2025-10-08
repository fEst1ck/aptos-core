#[test_only]
module supra_framework::aggregator_tests {

    use supra_framework::aggregator;
    use supra_framework::aggregator_factory;

    const MAX_U128: u128 = 340282366920938463463374607431768211455;

<<<<<<< HEAD:aptos-move/framework/supra-framework/tests/aggregator_tests.move
    #[test(account = @supra_framework)]
=======
    #[test(account = @aptos_framework)]
>>>>>>> aptos-framework-v1.34.0:aptos-move/framework/aptos-framework/tests/aggregator_tests.move
    fun test_can_add_and_sub_and_read(account: signer) {
        aggregator_factory::initialize_aggregator_factory_for_test(&account);
        let aggregator = aggregator_factory::create_aggregator_for_test();

        aggregator::add(&mut aggregator, 12);
        assert!(aggregator::read(&aggregator) == 12, 0);

        aggregator::add(&mut aggregator, 3);
        assert!(aggregator::read(&aggregator) == 15, 0);

        aggregator::add(&mut aggregator, 3);
        aggregator::add(&mut aggregator, 2);
        aggregator::sub(&mut aggregator, 20);
        assert!(aggregator::read(&aggregator) == 0, 0);

        aggregator::add(&mut aggregator, 1000);
        aggregator::sub(&mut aggregator, 1000);

        aggregator::destroy(aggregator);
    }

    #[test(account = @supra_framework)]
    #[expected_failure(abort_code = 0x020001, location = supra_framework::aggregator)]
    fun test_overflow(account: signer) {
        aggregator_factory::initialize_aggregator_factory_for_test(&account);
        let aggregator = aggregator_factory::create_aggregator_for_test();
        aggregator::add(&mut aggregator, MAX_U128 - 10);

        // Overflow!
        aggregator::add(&mut aggregator, 12);

        aggregator::destroy(aggregator);
    }

    #[test(account = @supra_framework)]
    #[expected_failure(abort_code = 0x020002, location = supra_framework::aggregator)]
    fun test_underflow(account: signer) {
        aggregator_factory::initialize_aggregator_factory_for_test(&account);
        let aggregator = aggregator_factory::create_aggregator_for_test();

        // Underflow!
        aggregator::sub(&mut aggregator, 100);
        aggregator::add(&mut aggregator, 100);

        aggregator::destroy(aggregator);
    }
}
