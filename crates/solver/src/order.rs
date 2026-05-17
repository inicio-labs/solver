use std::collections::HashSet;

use crate::types::TokenId;

/// A configured trading pair: the two faucet ids whose assets can be swapped.
///
/// In v1 (Option A), pairs are advisory — they seed the initial registered
/// token set; once tokens are registered the matcher freely matches across
/// any combination, and new tokens can be added at runtime via the admin API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AssetPair {
    pub x: TokenId,
    pub y: TokenId,
}

impl AssetPair {
    pub fn new(x: TokenId, y: TokenId) -> Self {
        Self { x, y }
    }
}

/// Flatten a slice of pairs into the unique token set used to seed
/// `PipelineConfig::initial_tokens`. Order is stable (first-seen wins) so
/// callers can rely on a deterministic ordering for downstream registration.
pub fn unique_tokens(pairs: &[AssetPair]) -> Vec<TokenId> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for p in pairs {
        if seen.insert(p.x) {
            out.push(p.x);
        }
        if seen.insert(p.y) {
            out.push(p.y);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_protocol::account::AccountId;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
    };

    fn token_a() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET).unwrap()
    }

    fn token_b() -> TokenId {
        AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap()
    }

    #[test]
    fn unique_tokens_dedupes_shared_token() {
        // Two pairs that share token_a: A-B and A-A (degenerate but valid).
        // Result should be [A, B], not [A, B, A, A].
        let a = token_a();
        let b = token_b();
        let pairs = vec![AssetPair::new(a, b), AssetPair::new(a, a)];
        let tokens = unique_tokens(&pairs);
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], a);
        assert_eq!(tokens[1], b);
    }

    #[test]
    fn unique_tokens_preserves_first_seen_order() {
        let a = token_a();
        let b = token_b();
        // First pair has B before A — output order: B, A.
        let pairs = vec![AssetPair::new(b, a)];
        let tokens = unique_tokens(&pairs);
        assert_eq!(tokens, vec![b, a]);
    }
}
