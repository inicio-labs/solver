//! `mock.toml` — everything the mirror daemon needs. Flat and explicit; no
//! defaults that could silently misbehave (a missing field is a loud load
//! error). See `mock.toml.example` for a filled-in template.

use anyhow::{bail, Context, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct MockConfig {
    pub rpc: RpcCfg,
    pub mock: MockAccountCfg,
    pub settings: SettingsCfg,
    /// Trading pairs to make a market on (both directions are subscribed).
    pub pairs: Vec<PairCfg>,
    /// Tokens the mock keeps funded; topped up from their faucet when low.
    pub inventory: Vec<InventoryCfg>,
}

#[derive(Debug, Deserialize)]
pub struct RpcCfg {
    pub endpoint: String,
    pub timeout_ms: u64,
}

/// Loose knobs grouped into one table so they can't be accidentally absorbed
/// into `[mock]` (TOML scalars after a table header belong to that table).
#[derive(Debug, Deserialize)]
pub struct SettingsCfg {
    /// Solver's account hex — added to the mirror denylist so the mock never
    /// mirrors notes the solver itself created. (The mock's own account is
    /// always denied too.)
    pub solver_account_id: String,
    /// Solver's edge per mirror, in basis points (e.g. 30 = 0.30%). 1..10000.
    pub spread_bps: u64,
    /// Probability [0.0, 1.0] that a given order is mirrored as a HALF fill
    /// (leaving a remainder) instead of a full fill.
    pub partial_fill_probability: f64,
    /// Cap on counter-orders created per tick, so a burst of user orders can't
    /// drain the pool before the claim loop catches up.
    pub max_mirrors_per_tick: usize,
    pub sync_interval_ms: u64,
    /// Seed for the (reproducible) full-vs-partial coin flip.
    pub seed: u64,
    /// Hard guardrail: must be explicitly `true` to run against a mainnet RPC.
    pub allow_mainnet: bool,
}

#[derive(Debug, Deserialize)]
pub struct MockAccountCfg {
    /// Hex account id of the (out-of-band provisioned) mock account.
    pub account_id: String,
    pub keystore_path: String,
    pub store_path: String,
}

#[derive(Debug, Deserialize)]
pub struct PairCfg {
    pub token_a_faucet_id: String,
    pub token_b_faucet_id: String,
}

#[derive(Debug, Deserialize)]
pub struct InventoryCfg {
    /// Faucet id of the token (the mock must control this faucet to mint).
    pub faucet_id: String,
    /// Mint a top-up when the mock's balance drops below this.
    pub low_water: u64,
    /// Amount to mint per top-up.
    pub topup: u64,
}

impl MockConfig {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read config {path}"))?;
        let cfg: MockConfig = toml::from_str(&text).context("parse mock.toml")?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.settings.spread_bps == 0 || self.settings.spread_bps >= 10_000 {
            bail!("spread_bps must be in 1..10000 (got {})", self.settings.spread_bps);
        }
        if !(0.0..=1.0).contains(&self.settings.partial_fill_probability) {
            bail!("partial_fill_probability must be in 0.0..=1.0");
        }
        if !self.settings.allow_mainnet && self.rpc.endpoint.to_lowercase().contains("mainnet") {
            bail!(
                "refusing to start: endpoint {:?} looks like mainnet and allow_mainnet=false. \
                 This is a TESTNET harness.",
                self.rpc.endpoint
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
        [rpc]
        endpoint = "ENDPOINT"
        timeout_ms = 1000
        [mock]
        account_id = "0x1"
        keystore_path = "k"
        store_path = "s"
        [settings]
        solver_account_id = "0x2"
        spread_bps = 30
        partial_fill_probability = 0.2
        max_mirrors_per_tick = 5
        sync_interval_ms = 1000
        seed = 1
        allow_mainnet = ALLOW
        [[pairs]]
        token_a_faucet_id = "0xa"
        token_b_faucet_id = "0xb"
        [[inventory]]
        faucet_id = "0xa"
        low_water = 1
        topup = 2
    "#;

    fn validate(endpoint: &str, allow_mainnet: bool) -> Result<()> {
        let text = BASE
            .replace("ENDPOINT", endpoint)
            .replace("ALLOW", &allow_mainnet.to_string());
        let cfg: MockConfig = toml::from_str(&text).unwrap();
        cfg.validate()
    }

    #[test]
    fn refuses_mainnet_unless_explicitly_allowed() {
        assert!(validate("http://rpc.mainnet.miden.io", false).is_err());
        assert!(validate("http://rpc.testnet.miden.io", false).is_ok());
        assert!(validate("http://rpc.mainnet.miden.io", true).is_ok());
    }
}
