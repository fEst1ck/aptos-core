module aa::test_functions {
<<<<<<< HEAD
    use supra_framework::aptos_account;
=======
    use aptos_framework::aptos_account;
>>>>>>> aptos-framework-v1.34.0

    /// test function for multi-agent aa.
    public entry fun transfer_to_the_last(a: &signer, b: &signer, c: &signer, d: address) {
        aptos_account::transfer(a, d, 1);
        aptos_account::transfer(b, d, 1);
        aptos_account::transfer(c, d, 1);
    }
}
