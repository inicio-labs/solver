//! On-chain consume helpers — **only compiled with the `consume` feature**.
//!
//! A `Handover` carries the PSWAP note as hex (`note_hex`). To act on it a
//! filler must (1) decode the bytes back into a miden [`Note`], (2) read the
//! swap terms so it knows what it pays and receives, and (3) build the consume
//! arguments for a (possibly partial) fill. This module does exactly those
//! three things and **nothing that needs a `miden-client`** — the filler runs
//! the transaction with its own client/keystore/gas. That keeps the default
//! SDK build free of every heavy miden dependency; enable `consume` only if you
//! want these turnkey helpers instead of wiring the bytes up yourself.
//!
//! ```ignore
//! use pswap_filler_sdk::consume::{decode_note, PswapTerms, consume_args};
//!
//! let note  = decode_note(&handover.note_hex)?;          // hex → Note
//! let terms = PswapTerms::from_note(&note)?;             // what am I getting / paying?
//! // ... your policy check against `terms` ...
//! let args  = consume_args(handover.fill_amount);        // partial-fill args (Word)
//! // feed `note` + `args` into your own miden-client TransactionRequestBuilder.
//! ```

use anyhow::{anyhow, Context, Result};
use miden_protocol::account::AccountId;
use miden_protocol::crypto::utils::{Deserializable, SliceReader};
use miden_protocol::note::Note;
use miden_protocol::Word;
use miden_standards::note::PswapNote;

/// Decode a hex-encoded serialized note (the `note_hex` field of a `Handover`)
/// back into a miden [`Note`]. Mirrors the solver's own
/// `Note::read_from(SliceReader::new(bytes))` path, so the bytes round-trip
/// exactly.
pub fn decode_note(note_hex: &str) -> Result<Note> {
    let trimmed = note_hex.strip_prefix("0x").unwrap_or(note_hex);
    let bytes = hex::decode(trimmed).context("note_hex is not valid hex")?;
    Note::read_from(&mut SliceReader::new(&bytes))
        .map_err(|e| anyhow!("failed to deserialize note: {e}"))
}

/// The economic terms of a PSWAP note, decoded for a filler's policy check.
///
/// The note **offers** `offered_amount` base units of `offered_faucet` and
/// **requests** `requested_amount` base units of `requested_faucet` in return.
/// A filler that consumes it receives the offered asset and must deliver the
/// requested asset (in full, or pro-rata for a partial fill). `creator` is the
/// account the requested asset (and any remainder) settles back to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PswapTerms {
    pub offered_faucet: AccountId,
    pub offered_amount: u64,
    pub requested_faucet: AccountId,
    pub requested_amount: u64,
    pub creator: AccountId,
}

impl PswapTerms {
    /// Parse the PSWAP terms out of a decoded note. Errors if the note is not a
    /// well-formed PSWAP note.
    pub fn from_note(note: &Note) -> Result<Self> {
        let pswap =
            PswapNote::try_from(note).map_err(|e| anyhow!("not a PSWAP note: {e}"))?;
        let offered = pswap.offered_asset();
        let storage = pswap.storage();
        let requested = storage.requested_asset();
        Ok(Self {
            offered_faucet: offered.faucet_id(),
            offered_amount: offered.amount().into(),
            requested_faucet: requested.faucet_id(),
            requested_amount: requested.amount().into(),
            creator: storage.creator_account_id(),
        })
    }
}

/// Build the note arguments for consuming a PSWAP note with a **partial fill**
/// of `note_fill` requested-token base units (pass the note's full
/// `requested_amount` for a complete fill). The returned [`Word`] is what the
/// filler passes as the note's args in its own transaction request — same value
/// the solver's executor uses (`PswapNote::create_args(0, note_fill)`).
pub fn consume_args(note_fill: u64) -> Result<Word> {
    PswapNote::create_args(0, note_fill).map_err(|e| anyhow!("failed to build consume args: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_rejects_bad_hex() {
        assert!(decode_note("zzzz").is_err());
        assert!(decode_note("0xnothex").is_err());
        // Valid hex but not a note → deserialize error, still no panic.
        assert!(decode_note("0xdead").is_err());
    }

    #[test]
    fn consume_args_is_deterministic() {
        // Same fill → same args; different fill → different args.
        assert_eq!(consume_args(1000).unwrap(), consume_args(1000).unwrap());
        assert_ne!(consume_args(1000).unwrap(), consume_args(2000).unwrap());
    }
}
