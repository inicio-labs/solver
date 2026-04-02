use anyhow::Result;
use miden_protocol::account::AccountId;
use miden_protocol::note::Note;
use miden_standards::note::PswapNote;

/// Asset pair with base (X) and quote (Y) faucet IDs.
/// Hash/Eq are order-independent so (X, Y) == (Y, X) as a map key.
#[derive(Debug, Clone)]
pub struct AssetPair {
    pub base: AccountId,
    pub quote: AccountId,
}

impl AssetPair {
    pub fn new(base: AccountId, quote: AccountId) -> Self {
        AssetPair { base, quote }
    }

    fn normalized(&self) -> (AccountId, AccountId) {
        if self.base < self.quote {
            (self.base, self.quote)
        } else {
            (self.quote, self.base)
        }
    }
}

impl PartialEq for AssetPair {
    fn eq(&self, other: &Self) -> bool {
        self.normalized() == other.normalized()
    }
}

impl Eq for AssetPair {}

impl std::hash::Hash for AssetPair {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.normalized().hash(state);
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
        let pswap = PswapNote::try_from(note)
            .map_err(|e| anyhow::anyhow!("Failed to parse PSWAP note: {}", e))?;

        let offered_asset = pswap.offered_asset();
        let offered_faucet_id = offered_asset.faucet_id();
        let offered_amount = offered_asset.amount();

        let requested_asset = pswap.storage().requested_asset();
        let requested_faucet_id = requested_asset.faucet_id();
        let requested_amount = requested_asset.amount();

        let creator_id = pswap.storage().creator_account_id();

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
