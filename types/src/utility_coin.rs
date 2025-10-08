// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::account_address::AccountAddress;
use move_core_types::{
    ident_str,
    identifier::IdentStr,
    language_storage::{StructTag, TypeTag},
    move_resource::MoveStructType,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

pub trait CoinType {
    fn type_tag() -> TypeTag;

    fn coin_info_address() -> AccountAddress;
}

<<<<<<< HEAD
pub static SUPRA_COIN_TYPE: Lazy<TypeTag> = Lazy::new(|| {
=======
static APTOS_COIN_TYPE: Lazy<TypeTag> = Lazy::new(|| {
>>>>>>> aptos-framework-v1.34.0
    TypeTag::Struct(Box::new(StructTag {
        address: AccountAddress::ONE,
        module: ident_str!("supra_coin").to_owned(),
        name: ident_str!("SupraCoin").to_owned(),
        type_args: vec![],
    }))
});

#[derive(Debug, Serialize, Deserialize)]
<<<<<<< HEAD
pub struct SupraCoinType;

impl CoinType for SupraCoinType {
    fn type_tag() -> TypeTag {
        SUPRA_COIN_TYPE.clone()
=======
pub struct AptosCoinType;

impl CoinType for AptosCoinType {
    fn type_tag() -> TypeTag {
        APTOS_COIN_TYPE.clone()
>>>>>>> aptos-framework-v1.34.0
    }

    fn coin_info_address() -> AccountAddress {
        AccountAddress::ONE
    }
}

<<<<<<< HEAD
impl MoveStructType for SupraCoinType {
    const MODULE_NAME: &'static IdentStr = ident_str!("supra_coin");
    const STRUCT_NAME: &'static IdentStr = ident_str!("SupraCoin");
=======
impl MoveStructType for AptosCoinType {
    const MODULE_NAME: &'static IdentStr = ident_str!("aptos_coin");
    const STRUCT_NAME: &'static IdentStr = ident_str!("AptosCoin");
>>>>>>> aptos-framework-v1.34.0
}

pub static DUMMY_COIN_TYPE: Lazy<TypeTag> = Lazy::new(|| {
    TypeTag::Struct(Box::new(StructTag {
        address: AccountAddress::ONE,
        module: ident_str!("dummy_coin").to_owned(),
        name: ident_str!("DummyCoin").to_owned(),
        type_args: vec![],
    }))
});

pub struct DummyCoinType;
impl CoinType for DummyCoinType {
    fn type_tag() -> TypeTag {
        DUMMY_COIN_TYPE.clone()
    }

    fn coin_info_address() -> AccountAddress {
        AccountAddress::ONE
    }
}
