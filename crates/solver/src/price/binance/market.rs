//! Which Binance Spot markets price which Miden pairs and tokens.
//!
//! The operator maps each Miden faucet to a Binance asset code and approves one
//! direct symbol per clearing pair. Public `exchangeInfo` then confirms each
//! symbol's base and quote assets and trading status; the pair's orientation
//! follows from that listing, never from a separate flag or from token names.

use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use thiserror::Error;

use crate::types::TokenId;

/// Longest accepted asset code; two codes always form a valid symbol.
const MAX_ASSET_LEN: usize = 16;
const MAX_SYMBOL_LEN: usize = 2 * MAX_ASSET_LEN;

/// Configuration errors, reported before the feed starts.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MarketError {
    #[error("invalid Binance asset code {0:?}: expected up to 16 ASCII letters and digits")]
    InvalidAssetCode(String),
    #[error("invalid Binance symbol {0:?}: expected up to 32 ASCII letters and digits")]
    InvalidSymbol(String),
    #[error("faucet {token} is mapped to Binance asset {first} and to {second}")]
    ConflictingAsset {
        token: TokenId,
        first: AssetCode,
        second: AssetCode,
    },
    #[error("pair {pair}: faucet {token} has no Binance asset code")]
    MissingAsset { pair: String, token: TokenId },
    #[error("pair {pair}: both sides are Binance asset {asset}")]
    SameAsset { pair: String, asset: AssetCode },
    #[error("pair {pair}: base and quote are the same faucet")]
    SameToken { pair: String },
    #[error("pair {pair}: the same two faucets are already priced by another pair")]
    DuplicatePair { pair: String },
}

/// Upper-cased `raw` if it is 1..=`max_len` ASCII letters and digits.
fn validate_name(raw: &str, max_len: usize) -> Option<String> {
    let name = raw.to_ascii_uppercase();
    (!name.is_empty()
        && name.len() <= max_len
        && name.bytes().all(|byte| byte.is_ascii_alphanumeric()))
    .then_some(name)
}

/// A Binance asset code such as `ETH`, `USDT` or `1000SATS` (upper case).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AssetCode(String);

impl AssetCode {
    pub fn parse(raw: &str) -> Result<Self, MarketError> {
        validate_name(raw, MAX_ASSET_LEN)
            .map(Self)
            .ok_or_else(|| MarketError::InvalidAssetCode(raw.to_owned()))
    }
}

impl fmt::Display for AssetCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A Binance Spot symbol such as `ETHUSDT` (upper case, as `bookTicker` and
/// `exchangeInfo` spell it).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Symbol(String);

impl Symbol {
    pub fn parse(raw: &str) -> Result<Self, MarketError> {
        validate_name(raw, MAX_SYMBOL_LEN)
            .map(Self)
            .ok_or_else(|| MarketError::InvalidSymbol(raw.to_owned()))
    }

    /// Binance's name for the `base`/`quote` market. Whether it exists, and
    /// trades exactly these assets, is for `exchangeInfo` to confirm.
    pub(crate) fn of_assets(base: &AssetCode, quote: &AssetCode) -> Self {
        Self(format!("{base}{quote}"))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }

    /// This symbol's stream in a combined-stream subscription.
    pub(crate) fn stream_name(&self) -> String {
        format!("{}@bookTicker", self.0.to_ascii_lowercase())
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Lets a `HashMap<Symbol, _>` be queried with the `&str` a frame carries.
impl Borrow<str> for Symbol {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// A symbol's public `exchangeInfo` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Listing {
    pub(crate) base_asset: AssetCode,
    pub(crate) quote_asset: AssetCode,
    /// `TRADING`, or a temporary state such as `BREAK` or `HALT`.
    pub(crate) status: String,
    /// `isSpotTradingAllowed`: false once a symbol is restricted ahead of a
    /// delisting, while its book may still stream.
    pub(crate) spot_trading_allowed: bool,
}

impl Listing {
    /// How this listing quotes the market `base`/`quote`, if it lists exactly
    /// those two assets.
    fn orientation(&self, base: &AssetCode, quote: &AssetCode) -> Option<Orientation> {
        if self.base_asset == *base && self.quote_asset == *quote {
            Some(Orientation::Direct)
        } else if self.base_asset == *quote && self.quote_asset == *base {
            Some(Orientation::Reverse)
        } else {
            None
        }
    }
}

/// How a Binance quote relates to a Miden pair `(base, quote)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Orientation {
    /// Binance's base asset is the pair's base: the price is `P`.
    Direct,
    /// Binance's base asset is the pair's quote: the price is `1 / P`.
    Reverse,
}

/// A Miden pair the solver clears, with its operator-approved Binance symbol.
#[derive(Clone, Debug)]
pub struct ClearingMarket {
    /// Configuration name, used in reports.
    pub name: String,
    pub base: TokenId,
    pub quote: TokenId,
    pub symbol: Symbol,
}

/// The markets the configuration asks for, before Binance has confirmed them.
#[derive(Clone, Debug)]
pub struct MarketPlan {
    /// Ordered, so valuation markets resolve in a stable order.
    assets: BTreeMap<TokenId, AssetCode>,
    clearing: Vec<ClearingMarket>,
    valuation_quote: AssetCode,
}

impl MarketPlan {
    /// `assets` maps faucets to Binance asset codes; a faucet may appear more
    /// than once only with the same code. Wallet valuation prices each mapped
    /// asset against `valuation_quote` (e.g. `ETHUSDT` for `ETH` in `USDT`).
    pub fn new(
        assets: impl IntoIterator<Item = (TokenId, AssetCode)>,
        clearing: Vec<ClearingMarket>,
        valuation_quote: AssetCode,
    ) -> Result<Self, MarketError> {
        let mut mapped: BTreeMap<TokenId, AssetCode> = BTreeMap::new();
        for (token, asset) in assets {
            match mapped.get(&token) {
                Some(first) if *first != asset => {
                    return Err(MarketError::ConflictingAsset {
                        token,
                        first: first.clone(),
                        second: asset,
                    })
                }
                Some(_) => {}
                None => {
                    mapped.insert(token, asset);
                }
            }
        }
        let mut pairs = BTreeSet::new();
        for market in &clearing {
            let pair = market.name.clone();
            if market.base == market.quote {
                return Err(MarketError::SameToken { pair });
            }
            let asset_of = |token: TokenId| {
                mapped.get(&token).ok_or_else(|| MarketError::MissingAsset {
                    pair: pair.clone(),
                    token,
                })
            };
            let (base_asset, quote_asset) = (asset_of(market.base)?, asset_of(market.quote)?);
            if base_asset == quote_asset {
                return Err(MarketError::SameAsset {
                    pair,
                    asset: base_asset.clone(),
                });
            }
            if !pairs.insert((market.base.min(market.quote), market.base.max(market.quote))) {
                return Err(MarketError::DuplicatePair { pair });
            }
        }
        Ok(Self {
            assets: mapped,
            clearing,
            valuation_quote,
        })
    }

    /// The valuation symbol for `asset`; `None` for the valuation quote itself.
    fn valuation_symbol(&self, asset: &AssetCode) -> Option<Symbol> {
        (*asset != self.valuation_quote).then(|| Symbol::of_assets(asset, &self.valuation_quote))
    }

    /// Every symbol to confirm through `exchangeInfo`, once each.
    pub(crate) fn symbols(&self) -> BTreeSet<Symbol> {
        let clearing = self.clearing.iter().map(|market| market.symbol.clone());
        let valuation = self
            .assets
            .values()
            .filter_map(|asset| self.valuation_symbol(asset));
        clearing.chain(valuation).collect()
    }

    /// Keep each use whose listing matches. `listings` holds every symbol from
    /// [`Self::symbols`]; `None` means Binance did not confirm it. Every issue
    /// is reported on its own; the other markets are unaffected. A market in a
    /// temporary non-`TRADING` state is still subscribed (its book sends
    /// nothing until trading resumes, so the TTL pauses it) and reported as a
    /// warning; see [`MarketIssue::rejects`].
    pub(crate) fn resolve(
        &self,
        listings: &HashMap<Symbol, Option<Listing>>,
    ) -> (Markets, Vec<MarketIssue>) {
        let mut markets = Markets::default();
        let mut issues = Vec::new();
        // The listing's orientation for `base`/`quote`, if it lists exactly
        // those assets and spot trading is allowed, plus a status warning.
        let check = |symbol: &Symbol, base: &AssetCode, quote: &AssetCode| {
            let listing = listings
                .get(symbol)
                .and_then(Option::as_ref)
                .ok_or(IssueKind::UnknownSymbol)?;
            let orientation =
                listing
                    .orientation(base, quote)
                    .ok_or_else(|| IssueKind::AssetMismatch {
                        base: listing.base_asset.clone(),
                        quote: listing.quote_asset.clone(),
                    })?;
            if !listing.spot_trading_allowed {
                return Err(IssueKind::SpotTradingNotAllowed);
            }
            let warning = (listing.status != "TRADING").then(|| IssueKind::NotTrading {
                status: listing.status.clone(),
            });
            Ok((orientation, warning))
        };
        let mut report = |symbol: &Symbol, use_: MarketUse, kind: IssueKind| {
            issues.push(MarketIssue {
                symbol: symbol.clone(),
                use_,
                kind,
            });
        };
        for market in &self.clearing {
            // `new` guarantees both assets are mapped.
            let (base, quote) = (&self.assets[&market.base], &self.assets[&market.quote]);
            let use_ = || MarketUse::Clearing(market.name.clone());
            match check(&market.symbol, base, quote) {
                Ok((orientation, warning)) => {
                    let symbol = markets.add_symbol(&market.symbol);
                    markets.pairs.insert(
                        (market.base, market.quote),
                        PairSource {
                            symbol,
                            orientation,
                        },
                    );
                    if let Some(kind) = warning {
                        report(&market.symbol, use_(), kind);
                    }
                }
                Err(kind) => report(&market.symbol, use_(), kind),
            }
        }
        for (&token, asset) in &self.assets {
            let Some(symbol) = self.valuation_symbol(asset) else {
                markets.valuation.insert(token, Valuation::Unit);
                continue;
            };
            // A valuation market quotes the token in the valuation asset.
            let checked =
                check(&symbol, asset, &self.valuation_quote).and_then(|(orientation, warning)| {
                    match orientation {
                        Orientation::Direct => Ok(warning),
                        Orientation::Reverse => Err(IssueKind::AssetMismatch {
                            base: self.valuation_quote.clone(),
                            quote: asset.clone(),
                        }),
                    }
                });
            match checked {
                Ok(warning) => {
                    let index = markets.add_symbol(&symbol);
                    markets.valuation.insert(token, Valuation::Market(index));
                    if let Some(kind) = warning {
                        report(&symbol, MarketUse::Valuation(token), kind);
                    }
                }
                Err(kind) => report(&symbol, MarketUse::Valuation(token), kind),
            }
        }
        (markets, issues)
    }
}

/// What a symbol was meant for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MarketUse {
    /// Internal clearing of the named pair.
    Clearing(String),
    /// Wallet valuation of a token.
    Valuation(TokenId),
}

impl fmt::Display for MarketUse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clearing(pair) => write!(f, "pair {pair}"),
            Self::Valuation(token) => write!(f, "valuation of {token}"),
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum IssueKind {
    #[error("Binance did not confirm this symbol")]
    UnknownSymbol,
    #[error("symbol status is {status}, not TRADING; subscribed, paused until it trades")]
    NotTrading { status: String },
    #[error("spot trading is not allowed on this symbol")]
    SpotTradingNotAllowed,
    #[error("listed as {base}/{quote}, which is not the configured market")]
    AssetMismatch { base: AssetCode, quote: AssetCode },
}

/// One configured market that Binance's listing does not fully support.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{use_} (symbol {symbol}): {kind}")]
pub(crate) struct MarketIssue {
    pub(crate) symbol: Symbol,
    pub(crate) use_: MarketUse,
    pub(crate) kind: IssueKind,
}

impl MarketIssue {
    /// Whether the market was left out of [`Markets`]. A non-`TRADING` status
    /// is only a warning: the symbol is subscribed and resumes on its own.
    pub(crate) fn rejects(&self) -> bool {
        !matches!(self.kind, IssueKind::NotTrading { .. })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PairSource {
    pub(crate) symbol: usize,
    pub(crate) orientation: Orientation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Valuation {
    /// The token is the valuation quote asset: worth exactly one.
    Unit,
    /// The symbol's midpoint, with the token as Binance's base asset.
    Market(usize),
}

/// Markets Binance has confirmed: the symbols to subscribe and what each prices.
#[derive(Clone, Debug, Default)]
pub(crate) struct Markets {
    symbols: Vec<Symbol>,
    /// Each symbol's combined-stream name, by index.
    streams: Vec<String>,
    index: HashMap<Symbol, usize>,
    /// Ordered, so the matcher clears pairs in a stable order.
    pairs: BTreeMap<(TokenId, TokenId), PairSource>,
    valuation: HashMap<TokenId, Valuation>,
}

impl Markets {
    fn add_symbol(&mut self, symbol: &Symbol) -> usize {
        if let Some(&index) = self.index.get(symbol) {
            return index;
        }
        let index = self.symbols.len();
        self.symbols.push(symbol.clone());
        self.streams.push(symbol.stream_name());
        self.index.insert(symbol.clone(), index);
        index
    }

    /// Symbols to subscribe, in index order.
    pub(crate) fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    /// The combined-stream name of the symbol at `index`.
    pub(crate) fn stream_name(&self, index: usize) -> &str {
        &self.streams[index]
    }

    /// Index of a subscribed symbol, spelled as `bookTicker` sends it.
    pub(crate) fn symbol_index(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    pub(crate) fn pair(&self, base: TokenId, quote: TokenId) -> Option<PairSource> {
        self.pairs.get(&(base, quote)).copied()
    }

    pub(crate) fn valuation(&self, token: TokenId) -> Option<Valuation> {
        self.valuation.get(&token).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::price::binance::test_support::{
        asset, btc, eth, listing, market, plan, symbol, usdt,
    };

    #[test]
    fn names_are_normalized_and_checked() {
        assert_eq!(asset("eth"), asset("ETH"));
        assert_eq!(symbol("ethUSDT").stream_name(), "ethusdt@bookTicker");
        assert!(AssetCode::parse(&"A".repeat(16)).is_ok());
        assert!(Symbol::parse(&"A".repeat(32)).is_ok());
        for invalid in ["", "ETH-USDT", "ETH USDT", "ÉTH", &"A".repeat(33)] {
            assert!(AssetCode::parse(invalid).is_err(), "{invalid}");
            assert!(Symbol::parse(invalid).is_err(), "{invalid}");
        }
        assert!(AssetCode::parse(&"A".repeat(17)).is_err());
        // Two maximal asset codes still form a valid symbol.
        let joined = Symbol::of_assets(&asset(&"A".repeat(16)), &asset(&"B".repeat(16)));
        assert_eq!(Symbol::parse(joined.as_str()), Ok(joined));
    }

    #[test]
    fn configuration_errors_are_rejected() {
        let conflict = MarketPlan::new(
            [(eth(), asset("ETH")), (eth(), asset("WETH"))],
            Vec::new(),
            asset("USDT"),
        );
        assert!(matches!(
            conflict,
            Err(MarketError::ConflictingAsset { .. })
        ));
        let assets = || [(eth(), asset("ETH")), (usdt(), asset("USDT"))];
        let missing = MarketPlan::new(
            [(eth(), asset("ETH"))],
            vec![market("ETH-USDT", eth(), usdt(), "ETHUSDT")],
            asset("USDT"),
        );
        assert!(matches!(missing, Err(MarketError::MissingAsset { .. })));
        let same_token = MarketPlan::new(
            assets(),
            vec![market("ETH-ETH", eth(), eth(), "ETHETH")],
            asset("USDT"),
        );
        assert!(matches!(same_token, Err(MarketError::SameToken { .. })));
        let same_asset = MarketPlan::new(
            [(eth(), asset("ETH")), (usdt(), asset("ETH"))],
            vec![market("ETH-ETH", eth(), usdt(), "ETHETH")],
            asset("USDT"),
        );
        assert!(matches!(same_asset, Err(MarketError::SameAsset { .. })));
        let duplicate = MarketPlan::new(
            assets(),
            vec![
                market("A", eth(), usdt(), "ETHUSDT"),
                market("B", usdt(), eth(), "ETHUSDT"),
            ],
            asset("USDT"),
        );
        assert!(matches!(duplicate, Err(MarketError::DuplicatePair { .. })));
    }

    #[test]
    fn orientation_comes_from_the_listing() {
        // Configured as USDT/ETH (base USDT), listed as ETH/USDT: reversed.
        let plan = plan(vec![
            market("USDT-ETH", usdt(), eth(), "ETHUSDT"),
            market("BTC-USDT", btc(), usdt(), "BTCUSDT"),
        ]);
        assert_eq!(
            plan.symbols().into_iter().collect::<Vec<_>>(),
            vec![symbol("BTCUSDT"), symbol("ETHUSDT")]
        );
        let listings = HashMap::from([
            (symbol("ETHUSDT"), Some(listing("ETH", "USDT"))),
            (symbol("BTCUSDT"), Some(listing("BTC", "USDT"))),
        ]);
        let (markets, issues) = plan.resolve(&listings);
        assert!(issues.is_empty(), "{issues:?}");
        let eth_usdt = markets.symbol_index("ETHUSDT").unwrap();
        assert_eq!(
            markets.pair(usdt(), eth()),
            Some(PairSource {
                symbol: eth_usdt,
                orientation: Orientation::Reverse
            })
        );
        assert_eq!(markets.pair(eth(), usdt()), None);
        assert_eq!(
            markets.pair(btc(), usdt()).unwrap().orientation,
            Orientation::Direct
        );
        // One subscription serves both clearing and valuation.
        assert_eq!(markets.symbols().len(), 2);
        assert_eq!(markets.stream_name(eth_usdt), "ethusdt@bookTicker");
        assert_eq!(markets.valuation(eth()), Some(Valuation::Market(eth_usdt)));
        assert_eq!(markets.valuation(usdt()), Some(Valuation::Unit));
    }

    #[test]
    fn each_rejected_market_is_reported_alone() {
        let plan = plan(vec![
            market("ETH-USDT", eth(), usdt(), "ETHUSDT"),
            market("BTC-ETH", btc(), eth(), "BTCETH"),
        ]);
        let mut restricted = listing("BTC", "USDT");
        restricted.spot_trading_allowed = false;
        let listings = HashMap::from([
            (symbol("ETHUSDT"), Some(listing("ETH", "USDT"))),
            // A listing for other assets under the configured name.
            (symbol("BTCETH"), Some(listing("BTC", "WETH"))),
            (symbol("BTCUSDT"), Some(restricted)),
        ]);
        let (markets, issues) = plan.resolve(&listings);
        assert!(markets.pair(eth(), usdt()).is_some());
        assert!(markets.pair(btc(), eth()).is_none());
        assert_eq!(markets.valuation(btc()), None);
        assert_eq!(markets.symbols(), &[symbol("ETHUSDT")]);
        assert_eq!(issues.len(), 2, "{issues:?}");
        assert!(issues.iter().all(MarketIssue::rejects));
        assert!(issues
            .iter()
            .any(|issue| issue.use_ == MarketUse::Clearing("BTC-ETH".into())
                && matches!(issue.kind, IssueKind::AssetMismatch { .. })));
        assert!(issues
            .iter()
            .any(|issue| issue.use_ == MarketUse::Valuation(btc())
                && issue.kind == IssueKind::SpotTradingNotAllowed));
        assert_eq!(
            issues[0].to_string(),
            "pair BTC-ETH (symbol BTCETH): listed as BTC/WETH, which is not the configured market"
        );
        let (_, unknown) = plan.resolve(&HashMap::new());
        assert!(unknown
            .iter()
            .all(|issue| issue.kind == IssueKind::UnknownSymbol));
        assert_eq!(unknown.len(), 4);
    }

    /// A halt is temporary: the symbol is subscribed and reported as a
    /// warning, so trading resumes without a restart.
    #[test]
    fn a_halted_market_is_subscribed_with_a_warning() {
        let plan = plan(vec![market("BTC-USDT", btc(), usdt(), "BTCUSDT")]);
        let mut halted = listing("BTC", "USDT");
        halted.status = "BREAK".into();
        let listings = HashMap::from([
            (symbol("BTCUSDT"), Some(halted)),
            (symbol("ETHUSDT"), Some(listing("ETH", "USDT"))),
        ]);
        let (markets, issues) = plan.resolve(&listings);
        let btc_usdt = markets.symbol_index("BTCUSDT").unwrap();
        assert_eq!(markets.pair(btc(), usdt()).unwrap().symbol, btc_usdt);
        assert_eq!(markets.valuation(btc()), Some(Valuation::Market(btc_usdt)));
        // One warning per use of the symbol, neither of them a rejection.
        assert_eq!(issues.len(), 2, "{issues:?}");
        assert!(issues.iter().all(|issue| !issue.rejects()
            && issue.kind
                == IssueKind::NotTrading {
                    status: "BREAK".into()
                }));
        assert_eq!(
            issues[0].to_string(),
            "pair BTC-USDT (symbol BTCUSDT): symbol status is BREAK, not TRADING; \
             subscribed, paused until it trades"
        );
    }

    #[test]
    fn valuation_requires_the_token_as_base() {
        // A listing quoting USDT in ETH is not a valuation of ETH in USDT.
        let plan = MarketPlan::new([(eth(), asset("ETH"))], Vec::new(), asset("USDT")).unwrap();
        let reversed = HashMap::from([(symbol("ETHUSDT"), Some(listing("USDT", "ETH")))]);
        let (markets, issues) = plan.resolve(&reversed);
        assert_eq!(markets.valuation(eth()), None);
        assert!(matches!(issues[0].kind, IssueKind::AssetMismatch { .. }));
    }
}
