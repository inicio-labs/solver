use anyhow::{Context, Result};
use miden_client::note::Note;
use miden_protocol::account::AccountId;
use miden_protocol::asset::Asset;
use miden_swapp::PswapNote;

/// Normalized asset pair key for grouping orders.
/// The pair is normalized so that (X, Y) == (Y, X) by sorting faucet IDs.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct AssetPair(pub AccountId, pub AccountId);

impl AssetPair {
    pub fn new(a: AccountId, b: AccountId) -> Self {
        if a < b {
            AssetPair(a, b)
        } else {
            AssetPair(b, a)
        }
    }
}

/// An order extracted from a PSWAP note.
#[derive(Debug, Clone)]
pub struct Order {
    pub note: Note,
    pub offered_faucet_id: AccountId,
    pub offered_amount: u64,
    pub requested_faucet_id: AccountId,
    pub requested_amount: u64,
    pub creator_id: AccountId,
    pub fill_amount: u64,
}

impl Order {
    /// Extract an Order from a PSWAP note.
    pub fn from_note(note: &Note) -> Result<Self> {
        let inputs = note.recipient().inputs();
        let input_values = inputs.values();

        // Get requested asset
        let requested_asset = PswapNote::get_requested_asset(input_values)
            .map_err(|e| anyhow::anyhow!("Failed to get requested asset: {}", e))?;

        let (requested_faucet_id, requested_amount) = match requested_asset {
            Asset::Fungible(fa) => (fa.faucet_id(), fa.amount()),
            _ => anyhow::bail!("Non-fungible requested asset not supported"),
        };

        // Get creator
        let creator_id = PswapNote::get_creator_account_id(input_values)
            .map_err(|e| anyhow::anyhow!("Failed to get creator account ID: {}", e))?;

        // Get offered asset from note assets
        let assets = note.assets();
        if assets.num_assets() != 1 {
            anyhow::bail!("PSWAP note must have exactly 1 offered asset");
        }
        let offered_asset = assets.iter().next().context("No offered asset")?;
        let (offered_faucet_id, offered_amount) = match offered_asset {
            Asset::Fungible(fa) => (fa.faucet_id(), fa.amount()),
            _ => anyhow::bail!("Non-fungible offered asset not supported"),
        };

        Ok(Order {
            note: note.clone(),
            offered_faucet_id,
            offered_amount,
            requested_faucet_id,
            requested_amount,
            creator_id,
            fill_amount: 0,
        })
    }

    /// Returns the normalized asset pair for this order.
    pub fn pair(&self) -> AssetPair {
        AssetPair::new(self.offered_faucet_id, self.requested_faucet_id)
    }

    /// Returns the remaining unfilled amount of the requested asset.
    pub fn remaining_requested(&self) -> u64 {
        self.requested_amount.saturating_sub(self.fill_amount)
    }

    /// Returns the effective price as requested/offered ratio (lower = cheaper = better for taker).
    pub fn price_ratio(&self) -> f64 {
        self.requested_amount as f64 / self.offered_amount as f64
    }
}
