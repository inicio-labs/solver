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

/// A trading pair as faucet account ids, from **your** (the filler's) side:
/// `offered` = the faucet of the token you give, `requested` = the faucet of the
/// token you want. A note you fill is the mirror — its offered asset is your
/// `requested` token, and vice versa.
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

// ── Client → server ──────────────────────────────────────────────────────────

/// Messages a DEX (client) sends to the router. A standing quote is the only
/// client message — **the quote is the registration**: its faucet ids imply the
/// pair, so there's no separate subscribe step. (Tagged for forward-compat: more
/// client messages can be added without a wire break.)
#[derive(Debug, Clone, PartialEq)]
pub enum ClientMsg {
    /// A standing quote, from the DEX's side: **give up to `offered` to receive
    /// `requested`** (offered = what you give, requested = what you want). The two
    /// assets carry both the rate (their ratio) and the max size, like a PSWAP
    /// note; their faucet ids imply the pair. Resend before expiry to refresh.
    Quote {
        offered: FungibleAsset,
        requested: FungibleAsset,
        /// Optional shorter validity (ms); capped at the server's quote TTL.
        valid_for_ms: Option<u64>,
    },
}

impl ClientMsg {
    const QUOTE: u8 = 0;
}

impl Serializable for ClientMsg {
    fn write_into<W: ByteWriter>(&self, target: &mut W) {
        match self {
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
// `Handover` carries a typed miden `Note` by value — deliberate (that's the point
// of the binary protocol). Frames are decoded one-at-a-time, never stored in bulk.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum ServerMsg {
    /// Handshake accepted; the connection is live.
    AuthOk,
    /// Reserved: the router asking the DEX to quote these pairs (pull model).
    /// Not currently emitted — the live flow is quote-driven (push).
    Ask { pairs: Vec<PairSpec> },
    /// A note to consume on-chain. The note enforces its own rate, so `note` +
    /// `fill_amount` fully specify the fill.
    Handover {
        /// The PSWAP note to consume — typed, not hex.
        note: Note,
        /// Requested-token base units to fill.
        fill_amount: u64,
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
            ServerMsg::Handover { note, fill_amount } => {
                target.write_u8(ServerMsg::HANDOVER);
                note.write_into(target);
                fill_amount.write_into(target);
            }
            ServerMsg::Error { code, msg } => {
                target.write_u8(ServerMsg::ERROR);
                code.write_into(target);
                msg.write_into(target);
            }
        }
    }
}

/// Build the raw `Handover` wire frame from an **already-serialized** note (as
/// ingest produced it, i.e. `note.to_bytes()`) plus `fill_amount`, without
/// decoding then re-encoding the note. Byte-identical to
/// `ServerMsg::Handover { note, fill_amount }.to_bytes()` whenever
/// `note_bytes == note.to_bytes()` (see the test).
///
/// The router carries PSWAP notes as opaque bytes end to end; this lets it emit
/// the typed-`Note` wire frame without ever parsing them (and without a wasteful
/// decode-then-re-encode round trip).
pub fn handover_frame(note_bytes: &[u8], fill_amount: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + note_bytes.len() + 8);
    out.write_u8(ServerMsg::HANDOVER);
    out.extend_from_slice(note_bytes);
    fill_amount.write_into(&mut out);
    out
}

impl Deserializable for ServerMsg {
    fn read_from<R: ByteReader>(source: &mut R) -> Result<Self, DeserializationError> {
        match source.read_u8()? {
            ServerMsg::AUTH_OK => Ok(ServerMsg::AuthOk),
            ServerMsg::ASK => Ok(ServerMsg::Ask { pairs: Vec::<PairSpec>::read_from(source)? }),
            ServerMsg::HANDOVER => Ok(ServerMsg::Handover {
                note: Note::read_from(source)?,
                fill_amount: u64::read_from(source)?,
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
        use miden_protocol::account::AccountId;
        // A Quote exercises the tag + FungibleAsset + Option framing.
        let a = AccountId::from_hex("0x4a03c1843860c9b17582c021d563ae").unwrap();
        let b = AccountId::from_hex("0x2458e5446128e6b150b75b8ebd9ce1").unwrap();
        let q = ClientMsg::Quote {
            offered: FungibleAsset::new(a, 1_000).unwrap(),
            requested: FungibleAsset::new(b, 2_000).unwrap(),
            valid_for_ms: Some(5_000),
        };
        let back = ClientMsg::read_from_bytes(&q.to_bytes()).unwrap();
        assert_eq!(q, back);
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

    #[test]
    fn handover_frame_matches_manual_wire_layout() {
        // We can't cheaply construct a real `Note` here, so lock the frame
        // layout instead: [HANDOVER tag] ++ opaque note bytes ++ fill_amount.
        // (The router relies on this being byte-identical to a re-serialized
        // `ServerMsg::Handover` when the bytes are a real `note.to_bytes()`.)
        let note_bytes = b"opaque-serialized-note-bytes";
        let frame = handover_frame(note_bytes, 7);
        assert_eq!(frame[0], ServerMsg::HANDOVER);
        assert_eq!(&frame[1..1 + note_bytes.len()], note_bytes);
        assert_eq!(u64::read_from_bytes(&frame[1 + note_bytes.len()..]).unwrap(), 7);
    }

    #[test]
    fn handover_frame_is_byte_identical_to_serialized_handover() {
        // The load-bearing invariant: building the frame from an already-serialized
        // note must equal serializing a typed `ServerMsg::Handover` — and decode
        // back to the same note. Exercised here with a REAL note (not fake bytes),
        // so the router's opaque-bytes shortcut is guarded inside the SDK itself.
        use miden_protocol::note::Note;
        use miden_protocol::Word;
        let note = Note::mock_noop(Word::from([1u32, 2, 3, 4]));
        let framed = handover_frame(&note.to_bytes(), 42);
        let typed = ServerMsg::Handover { note: note.clone(), fill_amount: 42 }.to_bytes();
        assert_eq!(framed, typed, "raw-frame builder must match typed serialization");
        match ServerMsg::read_from_bytes(&framed).unwrap() {
            ServerMsg::Handover { note: got, fill_amount } => {
                assert_eq!(got.id(), note.id());
                assert_eq!(fill_amount, 42);
            }
            other => panic!("expected Handover, got {other:?}"),
        }
    }

    #[test]
    fn ask_and_pairspec_round_trip() {
        // The reserved Ask path (the only carrier of PairSpec on the wire) had no
        // round-trip coverage — a framing bug there would be silent.
        use miden_protocol::account::AccountId;
        let a = AccountId::from_hex("0x4a03c1843860c9b17582c021d563ae").unwrap();
        let b = AccountId::from_hex("0x2458e5446128e6b150b75b8ebd9ce1").unwrap();
        let ask = ServerMsg::Ask {
            pairs: vec![
                PairSpec { offered: a, requested: b },
                PairSpec { offered: b, requested: a },
            ],
        };
        match ServerMsg::read_from_bytes(&ask.to_bytes()).unwrap() {
            ServerMsg::Ask { pairs } => {
                assert_eq!(pairs.len(), 2);
                assert_eq!((pairs[0].offered, pairs[0].requested), (a, b));
                assert_eq!((pairs[1].offered, pairs[1].requested), (b, a));
            }
            other => panic!("expected Ask, got {other:?}"),
        }
    }
}
