//! Binary RFQ wire protocol — the shared contract between the solver's router
//! and a filler.
//!
//! Messages serialize with miden's `Serializable`/`Deserializable` (compact
//! binary) and travel over WebSocket **binary** frames, so miden types
//! (`AccountId`, `FungibleAsset`, `Note`) go on the wire natively — no serde, no
//! hex, no string parsing. The router and this SDK both build on this module, so
//! the two sides can never drift.

use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::{
    ByteReader, ByteWriter, Deserializable, DeserializationError, Serializable,
};
use miden_protocol::note::Note;

/// A trading pair as faucet account ids, in the note's `(offered, requested)`
/// orientation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairSpec {
    pub offered: AccountId,
    pub requested: AccountId,
}

impl Serializable for PairSpec {
    fn write_into<W: ByteWriter>(&self, target: &mut W) {
        self.offered.write_into(target);
        self.requested.write_into(target);
    }
}

impl Deserializable for PairSpec {
    fn read_from<R: ByteReader>(source: &mut R) -> Result<Self, DeserializationError> {
        Ok(PairSpec {
            offered: AccountId::read_from(source)?,
            requested: AccountId::read_from(source)?,
        })
    }
}

/// A price as an exact rational `num / den` (requested-token per offered-token).
/// Integer-native — no decimal strings, no float on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PriceRatio {
    pub num: u64,
    pub den: u64,
}

impl Serializable for PriceRatio {
    fn write_into<W: ByteWriter>(&self, target: &mut W) {
        self.num.write_into(target);
        self.den.write_into(target);
    }
}

impl Deserializable for PriceRatio {
    fn read_from<R: ByteReader>(source: &mut R) -> Result<Self, DeserializationError> {
        Ok(PriceRatio {
            num: u64::read_from(source)?,
            den: u64::read_from(source)?,
        })
    }
}

// ── Client → server ──────────────────────────────────────────────────────────

/// Messages a DEX (client) sends to the router.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientMsg {
    /// Pairs the DEX can fill (for `Ask` targeting; quotes still gate per pair).
    Subscribe { pairs: Vec<PairSpec> },
    /// A standing quote: the DEX will give up to `offered` for `requested`. The
    /// two assets carry both the rate (their ratio) and the max size, exactly
    /// like a PSWAP note. Resend before expiry to refresh.
    Quote {
        offered: FungibleAsset,
        requested: FungibleAsset,
        /// Optional shorter validity (ms); capped at the server's quote TTL.
        valid_for_ms: Option<u64>,
    },
}

impl ClientMsg {
    const SUBSCRIBE: u8 = 0;
    const QUOTE: u8 = 1;
}

impl Serializable for ClientMsg {
    fn write_into<W: ByteWriter>(&self, target: &mut W) {
        match self {
            ClientMsg::Subscribe { pairs } => {
                target.write_u8(ClientMsg::SUBSCRIBE);
                pairs.write_into(target);
            }
            ClientMsg::Quote { offered, requested, valid_for_ms } => {
                target.write_u8(ClientMsg::QUOTE);
                offered.write_into(target);
                requested.write_into(target);
                valid_for_ms.write_into(target);
            }
        }
    }
}

impl Deserializable for ClientMsg {
    fn read_from<R: ByteReader>(source: &mut R) -> Result<Self, DeserializationError> {
        match source.read_u8()? {
            ClientMsg::SUBSCRIBE => {
                Ok(ClientMsg::Subscribe { pairs: Vec::<PairSpec>::read_from(source)? })
            }
            ClientMsg::QUOTE => Ok(ClientMsg::Quote {
                offered: FungibleAsset::read_from(source)?,
                requested: FungibleAsset::read_from(source)?,
                valid_for_ms: Option::<u64>::read_from(source)?,
            }),
            tag => Err(DeserializationError::InvalidValue(format!("unknown ClientMsg tag {tag}"))),
        }
    }
}

// ── Server → client ──────────────────────────────────────────────────────────

/// Messages the router sends to a DEX.
#[derive(Debug, Clone)]
pub enum ServerMsg {
    /// Handshake accepted; the connection is live.
    AuthOk,
    /// Reserved: the router asking the DEX to quote these pairs (pull model).
    /// Not currently emitted — the live flow is quote-driven (push).
    Ask { pairs: Vec<PairSpec> },
    /// A note to consume on-chain, at the matched price.
    Handover {
        /// The PSWAP note to consume — typed, not hex.
        note: Note,
        /// Requested-token base units to fill.
        fill_amount: u64,
        /// The price the match used (the DEX's own quote, echoed) — fill at this
        /// rate. `num/den` = requested-per-offered.
        fill_price: PriceRatio,
    },
    /// A structured error (e.g. a malformed quote was rejected).
    Error { code: String, msg: String },
}

impl ServerMsg {
    const AUTH_OK: u8 = 0;
    const ASK: u8 = 1;
    const HANDOVER: u8 = 2;
    const ERROR: u8 = 3;
}

impl Serializable for ServerMsg {
    fn write_into<W: ByteWriter>(&self, target: &mut W) {
        match self {
            ServerMsg::AuthOk => target.write_u8(ServerMsg::AUTH_OK),
            ServerMsg::Ask { pairs } => {
                target.write_u8(ServerMsg::ASK);
                pairs.write_into(target);
            }
            ServerMsg::Handover { note, fill_amount, fill_price } => {
                target.write_u8(ServerMsg::HANDOVER);
                note.write_into(target);
                fill_amount.write_into(target);
                fill_price.write_into(target);
            }
            ServerMsg::Error { code, msg } => {
                target.write_u8(ServerMsg::ERROR);
                code.write_into(target);
                msg.write_into(target);
            }
        }
    }
}

impl Deserializable for ServerMsg {
    fn read_from<R: ByteReader>(source: &mut R) -> Result<Self, DeserializationError> {
        match source.read_u8()? {
            ServerMsg::AUTH_OK => Ok(ServerMsg::AuthOk),
            ServerMsg::ASK => Ok(ServerMsg::Ask { pairs: Vec::<PairSpec>::read_from(source)? }),
            ServerMsg::HANDOVER => Ok(ServerMsg::Handover {
                note: Note::read_from(source)?,
                fill_amount: u64::read_from(source)?,
                fill_price: PriceRatio::read_from(source)?,
            }),
            ServerMsg::ERROR => Ok(ServerMsg::Error {
                code: String::read_from(source)?,
                msg: String::read_from(source)?,
            }),
            tag => Err(DeserializationError::InvalidValue(format!("unknown ServerMsg tag {tag}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_msg_binary_round_trip() {
        // Subscribe with an empty pair list exercises the tag + Vec framing
        // without needing miden asset fixtures.
        let sub = ClientMsg::Subscribe { pairs: vec![] };
        let back = ClientMsg::read_from_bytes(&sub.to_bytes()).unwrap();
        assert_eq!(sub, back);
    }

    #[test]
    fn price_ratio_round_trip() {
        let p = PriceRatio { num: 205, den: 100 };
        assert_eq!(p, PriceRatio::read_from_bytes(&p.to_bytes()).unwrap());
    }

    #[test]
    fn server_error_round_trips_and_tag_dispatches() {
        let e = ServerMsg::Error { code: "bad_quote".into(), msg: "nope".into() };
        match ServerMsg::read_from_bytes(&e.to_bytes()).unwrap() {
            ServerMsg::Error { code, msg } => {
                assert_eq!(code, "bad_quote");
                assert_eq!(msg, "nope");
            }
            other => panic!("expected Error, got {other:?}"),
        }
        // AuthOk is a bare tag.
        assert!(matches!(
            ServerMsg::read_from_bytes(&ServerMsg::AuthOk.to_bytes()).unwrap(),
            ServerMsg::AuthOk
        ));
    }

    #[test]
    fn unknown_tag_is_error_not_panic() {
        assert!(ClientMsg::read_from_bytes(&[99]).is_err());
        assert!(ServerMsg::read_from_bytes(&[99]).is_err());
    }
}
