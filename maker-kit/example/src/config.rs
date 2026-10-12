//! `maker.toml`: endpoints, wallet paths and the token table.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use miden_protocol::account::AccountId;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub network: Network,
    pub gateway: Gateway,
    pub wallet: Wallet,
    pub state: State,
    pub tokens: Vec<Token>,
}

#[derive(Debug, Deserialize)]
pub struct Network {
    pub rpc: String,
    pub rpc_timeout_ms: u64,
    pub prover: Option<String>,
    pub prover_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
pub struct Gateway {
    pub url: String,
    pub api_key_file: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct Wallet {
    pub store: PathBuf,
    pub keystore: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct State {
    pub journal: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Token {
    pub symbol: String,
    pub faucet: String,
    pub decimals: u32,
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
        toml::from_str(&text).with_context(|| format!("parse {path}"))
    }

    pub fn api_key(&self) -> Result<String> {
        let key = std::fs::read_to_string(&self.gateway.api_key_file)
            .with_context(|| format!("read {}", self.gateway.api_key_file.display()))?;
        Ok(key.trim().to_string())
    }

    pub fn token(&self, symbol: &str) -> Result<&Token> {
        self.tokens
            .iter()
            .find(|t| t.symbol.eq_ignore_ascii_case(symbol))
            .with_context(|| format!("unknown token {symbol}; known: {}", self.symbols()))
    }

    fn symbols(&self) -> String {
        self.tokens.iter().map(|t| t.symbol.as_str()).collect::<Vec<_>>().join(", ")
    }
}

impl Token {
    pub fn faucet_id(&self) -> Result<AccountId> {
        AccountId::from_hex(&self.faucet).with_context(|| format!("faucet id of {}", self.symbol))
    }

    /// "1.5" whole tokens -> base units, exactly (no floating point).
    pub fn to_base_units(&self, whole: &str) -> Result<u64> {
        let (int, frac) = whole.split_once('.').unwrap_or((whole, ""));
        if frac.len() > self.decimals as usize {
            bail!("{whole} has more than {} decimals for {}", self.decimals, self.symbol);
        }
        let digits = format!("{int}{frac:0<width$}", width = self.decimals as usize);
        let units: u64 = digits.parse().with_context(|| format!("bad amount {whole}"))?;
        if units == 0 {
            bail!("amount must be positive");
        }
        Ok(units)
    }

    /// Base units -> whole tokens, for display.
    pub fn to_whole(&self, units: u64) -> String {
        let scale = 10u64.pow(self.decimals);
        let frac = format!("{:0width$}", units % scale, width = self.decimals as usize);
        let frac = frac.trim_end_matches('0');
        if frac.is_empty() {
            format!("{}", units / scale)
        } else {
            format!("{}.{frac}", units / scale)
        }
    }
}
