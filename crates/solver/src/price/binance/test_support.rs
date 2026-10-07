//! Fixtures shared by the Binance price tests.

use std::collections::HashMap;

use rust_decimal::Decimal;

use miden_protocol::account::AccountId;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
};

use super::market::{AssetCode, ClearingMarket, IssueKind, Listing, MarketPlan, Markets, Symbol};
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

/// On-chain decimals of the test tokens: ETH 18, USDT 6, BTC 8.
pub(crate) fn decimals() -> HashMap<TokenId, u8> {
    HashMap::from([(eth(), 18), (usdt(), 6), (btc(), 8)])
}

pub(crate) fn price(raw: &str) -> Decimal {
    Decimal::from_str_exact(raw).unwrap()
}

pub(crate) fn asset(code: &str) -> AssetCode {
    AssetCode::parse(code).unwrap()
}

pub(crate) fn symbol(name: &str) -> Symbol {
    Symbol::parse(name).unwrap()
}

/// A `TRADING` spot listing of `base`/`quote`.
pub(crate) fn listing(base: &str, quote: &str) -> Listing {
    Listing {
        base_asset: asset(base),
        quote_asset: asset(quote),
        status: "TRADING".into(),
        spot_trading_allowed: true,
    }
}

/// A clearing pair `base`/`quote` approved on `symbol_name`.
pub(crate) fn market(
    name: &str,
    base: TokenId,
    quote: TokenId,
    symbol_name: &str,
) -> ClearingMarket {
    ClearingMarket {
        name: name.into(),
        base,
        quote,
        symbol: symbol(symbol_name),
    }
}

/// The ETH, USDT and BTC faucets mapped to their Binance assets, with their
/// decimals, valued in USDT, clearing `clearing`.
pub(crate) fn plan(clearing: Vec<ClearingMarket>) -> MarketPlan {
    MarketPlan::new(
        [
            (eth(), asset("ETH")),
            (usdt(), asset("USDT")),
            (btc(), asset("BTC")),
        ],
        clearing,
        asset("USDT"),
    )
    .unwrap()
    .with_decimals(decimals())
}

/// `plan` resolved against `TRADING` listings `(symbol, base, quote)`. Symbols
/// the plan asks for but `listings` leaves out are simply not confirmed; any
/// other issue is a test error.
pub(crate) fn confirmed(plan: &MarketPlan, listings: &[(&str, &str, &str)]) -> Markets {
    let listings: HashMap<_, _> = listings
        .iter()
        .map(|&(name, base, quote)| (symbol(name), Some(listing(base, quote))))
        .collect();
    let (markets, issues) = plan.resolve(&listings);
    let unexpected: Vec<_> = issues
        .iter()
        .filter(|issue| {
            issue.kind != IssueKind::UnknownSymbol || listings.contains_key(&issue.symbol)
        })
        .collect();
    assert!(unexpected.is_empty(), "{unexpected:?}");
    markets
}

/// Markets for the ETH faucet's USDT valuation only.
pub(crate) fn eth_valuation_markets() -> Markets {
    let plan = MarketPlan::new([(eth(), asset("ETH"))], Vec::new(), asset("USDT")).unwrap();
    confirmed(&plan, &[("ETHUSDT", "ETH", "USDT")])
}

/// USDT/ETH configured with USDT as base, priced by `ETHUSDT` (reversed).
pub(crate) fn reversed_eth_markets() -> Markets {
    let plan = MarketPlan::new(
        [(eth(), asset("ETH")), (usdt(), asset("USDT"))],
        vec![market("USDT-ETH", usdt(), eth(), "ETHUSDT")],
        asset("USDT"),
    )
    .unwrap()
    .with_decimals(decimals());
    confirmed(&plan, &[("ETHUSDT", "ETH", "USDT")])
}
