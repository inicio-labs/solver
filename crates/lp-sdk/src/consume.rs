//! On-chain consume helpers.
//!
//! A [`Handover`](crate::client::Handover) carries a decoded miden [`Note`]
//! directly (no hex), so a filler just needs to (1) read the swap terms to know
//! what it pays and receives, and (2) build the consume note-args for a
//! (possibly partial) fill. Read the terms straight off
//! [`miden_standards::note::PswapNote`] (re-exported here); nothing here needs a
//! `miden-client` — the filler runs the transaction with its own client/gas.
//!
//! ```ignore
//! use pswap_lp_sdk::consume::{consume_args, PswapNote};
//!
//! let pswap = PswapNote::try_from(&handover.note)?;      // what am I getting / paying?
//! // ... your policy check against pswap.offered_asset() / requested_asset() ...
//! let args = consume_args(0, handover.fill_amount)?;     // (account_fill, note_fill) → Word
//! // feed `handover.note` + `args` into your own miden-client TransactionRequestBuilder.
//! ```

use miden_protocol::Word;

use crate::client::LpError;

pub use miden_standards::note::PswapNote;

/// Build the note arguments for consuming a PSWAP note.
///
/// - `account_fill` — amount filled from the **consuming account's** side.
/// - `note_fill` — requested-token base units filled from the **note**.
///
/// For a complete note-side fill, pass `account_fill = 0` and the note's full
/// requested amount as `note_fill`. The returned [`Word`] is what the filler
/// passes as the note's args in its own transaction request — the same value the
/// solver's executor uses (`PswapNote::create_args(account_fill, note_fill)`).
pub fn consume_args(account_fill: u64, note_fill: u64) -> Result<Word, LpError> {
    PswapNote::create_args(account_fill, note_fill).map_err(|e| LpError::Consume(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consume_args_is_deterministic() {
        // Same fills → same args; different fills → different args.
        assert_eq!(consume_args(0, 1000).unwrap(), consume_args(0, 1000).unwrap());
        assert_ne!(consume_args(0, 1000).unwrap(), consume_args(0, 2000).unwrap());
        // account_fill is a distinct dimension now, not hardcoded to 0.
        assert_ne!(consume_args(0, 1000).unwrap(), consume_args(5, 1000).unwrap());
    }
}
