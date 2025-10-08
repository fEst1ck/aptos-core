// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

<<<<<<< HEAD
use crate::MoveHarness;
use aptos_cached_packages::aptos_stdlib::supra_account_transfer;
=======
use crate::{assert_success, MoveHarness};
use aptos_cached_packages::aptos_stdlib::aptos_account_transfer;
>>>>>>> aptos-framework-v1.34.0
use aptos_language_e2e_tests::account::Account;

#[test]
fn non_existent_sender() {
    let mut h = MoveHarness::new();

    let sender = Account::new();
    let receiver = h.new_account_with_balance_and_sequence_number(100_000, 0);

    let txn = sender
        .transaction()
<<<<<<< HEAD
        .payload(supra_account_transfer(*receiver.address(), 10))
=======
        .payload(aptos_account_transfer(*receiver.address(), 0))
>>>>>>> aptos-framework-v1.34.0
        .sequence_number(0)
        .sign();

    let status = h.run(txn);
    assert_success!(status);
}
