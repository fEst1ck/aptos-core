#[test_only]
<<<<<<< HEAD:aptos-move/framework/supra-framework/tests/account_abstraction_tests.move
module supra_framework::account_abstraction_tests {
    use std::signer;
    use supra_framework::auth_data::AbstractionAuthData;
    use supra_framework::object;
=======
module aptos_framework::account_abstraction_tests {
    use std::signer;
    use aptos_framework::auth_data::AbstractionAuthData;
    use aptos_framework::object;
>>>>>>> aptos-framework-v1.34.0:aptos-move/framework/aptos-framework/tests/account_abstraction_tests.move

    public fun invalid_authenticate(
        account: signer,
        _signing_data: AbstractionAuthData,
    ): signer {
        let addr = signer::address_of(&account);
        let cref = object::create_object(addr);
        object::generate_signer(&cref)
    }

    public fun test_auth(account: signer, _data: AbstractionAuthData): signer { account }
}
