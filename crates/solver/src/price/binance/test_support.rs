//! Fixtures shared by the Binance price tests.

use miden_protocol::account::AccountId;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
};

use super::market::{AssetCode, Listing, Symbol};
use crate::clearing::ReferencePrice;
use crate::types::TokenId;

pub(crate) fn eth() -> TokenId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
}

pub(crate) fn usdt() -> TokenId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
}

pub(crate) fn btc() -> TokenId {
    AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2).unwrap()
}

pub(crate) fn price(raw: &str) -> ReferencePrice {
    ReferencePrice::from_decimal(raw).unwrap()
}

pub(crate) fn asset(code: &str) -> AssetCode {
    AssetCode::parse(code).unwrap()
}

pub(crate) fn symbol(name: &str) -> Symbol {
    Symbol::parse(name).unwrap()
}

/// A `TRADING` listing of `base`/`quote`.
pub(crate) fn listing(base: &str, quote: &str) -> Listing {
    Listing {
        base_asset: asset(base),
        quote_asset: asset(quote),
        status: "TRADING".into(),
    }
}
