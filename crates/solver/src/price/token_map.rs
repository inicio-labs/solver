//! Oracle-agnostic token → external-symbol map shared across the solver.
//!
//! Maps a faucet [`TokenId`] to the symbol/id a price oracle knows it by (for
//! example a CoinGecko id). Hydrated from the DB once at boot and kept current
//! by the admin register/remove handlers (write-through), so any price oracle
//! reads the current set from memory without a DB round-trip. The CoinGecko
//! client is one consumer of this map, not its owner.

use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::types::TokenId;

/// Shared, in-memory faucet-id → external-symbol map (see module docs).
pub type SharedTokenMap = Arc<RwLock<HashMap<TokenId, String>>>;

/// Acquire a read guard on the shared token map, recovering from lock
/// poisoning instead of panicking.
///
/// A `std::sync::RwLock` stays poisoned permanently once any thread panics
/// while holding it, so `.read().expect(..)` would turn one unrelated panic
/// into a *permanent* crash source on every subsequent price fetch / admin
/// call. The protected value is only a `HashMap<TokenId, String>`; a panic by
/// a prior holder cannot leave it in an invariant-violating state, so
/// recovering the guard via `PoisonError::into_inner()` is strictly safe and
/// makes poisoning a non-event.
pub fn read_token_map(m: &SharedTokenMap) -> RwLockReadGuard<'_, HashMap<TokenId, String>> {
    m.read().unwrap_or_else(|e| e.into_inner())
}

/// Write-guard counterpart of [`read_token_map`]; same poison-recovery
/// rationale.
pub fn write_token_map(m: &SharedTokenMap) -> RwLockWriteGuard<'_, HashMap<TokenId, String>> {
    m.write().unwrap_or_else(|e| e.into_inner())
}
