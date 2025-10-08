#[test_only]
<<<<<<< HEAD:aptos-move/framework/supra-framework/tests/native_disaptch_token_tests.move
module supra_framework::native_dispatch_token_tests {
    use supra_framework::fungible_asset;
    use 0xcafe::native_dispatch_token;

    #[test(creator = @0xcafe)]
    #[expected_failure(abort_code=0x10019, location=supra_framework::fungible_asset)]
=======
module aptos_framework::native_dispatch_token_tests {
    use aptos_framework::fungible_asset;
    use 0xcafe::native_dispatch_token;

    #[test(creator = @0xcafe)]
    #[expected_failure(abort_code=0x10019, location=aptos_framework::fungible_asset)]
>>>>>>> aptos-framework-v1.34.0:aptos-move/framework/aptos-framework/tests/native_disaptch_token_tests.move
    fun test_native_dispatch_token(
        creator: &signer,
    ) {
        let (creator_ref, _) = fungible_asset::create_test_token(creator);
        fungible_asset::init_test_metadata(&creator_ref);

        native_dispatch_token::initialize(creator, &creator_ref);
    }
}
