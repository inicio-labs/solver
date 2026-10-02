//! Validated rows for the PostgreSQL application database.
//!
//! An order's terms live only in its serialized note (`raw_data`). Ordinary
//! orders have no `priority_seq` insertion field: the database assigns it.
//! Only a settlement remainder inherits its parent's priority.

use std::sync::Arc;

use diesel::prelude::*;
use miden_client::transaction::TransactionResult;
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::note::Note;

use super::error::{DbError, DbResult};
use super::postgres_schema::{orders, registered_tokens, settlement_attempts, settlement_inputs};
use crate::types::{BookOrder, Order, OrderId, SettlementError};

/// An unresolved settlement, stored as its snake_case name. Confirmed and
/// released attempts are deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum SettlementStatus {
    /// Proven and reserved; may be (re)submitted.
    Prepared,
    /// A resubmission was rejected but the first copy may have landed: do
    /// not resubmit, wait for the node to settle the transaction ID.
    Uncertain,
    /// Rejected at submission: it never entered the chain.
    Rejected,
}

impl SettlementStatus {
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = orders)]
pub struct OrderRow {
    pub note_id: Vec<u8>,
    pub raw_data: Vec<u8>,
    pub arrival_unix: i64,
    pub status: String,
    pub priority_seq: i64,
}

impl OrderRow {
    pub fn note(&self) -> DbResult<Note> {
        Ok(Note::read_from(&mut SliceReader::new(&self.raw_data))?)
    }

    /// Only a hand-edited or corrupted row fails the checks below: every
    /// write derives `note_id` from the note and the priority from PostgreSQL.
    pub fn into_book_order(self) -> DbResult<BookOrder> {
        let note = self.note()?;
        let note_id = OrderId::read_from(&mut SliceReader::new(&self.note_id))?;
        if note.id() != note_id {
            return Err(DbError::Corrupt("stored order ID differs from its note"));
        }
        let priority_seq = u64::try_from(self.priority_seq)?;
        if priority_seq == 0 {
            return Err(DbError::Corrupt("stored order lacks a FIFO priority"));
        }
        Ok(BookOrder {
            priority_seq,
            arrival_unix: u64::try_from(self.arrival_unix)?,
            note: Arc::new(note),
        })
    }
}

/// A fresh external order. PostgreSQL's identity column is never supplied.
#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = orders)]
pub struct NewOrderRow {
    pub note_id: Vec<u8>,
    pub raw_data: Vec<u8>,
    pub arrival_unix: i64,
}

/// A settlement child inherits the exact FIFO slot and arrival time of its
/// parent; it is the only kind of new order that supplies `priority_seq`.
#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = orders)]
pub struct NewRemainderOrderRow {
    pub note_id: Vec<u8>,
    pub raw_data: Vec<u8>,
    pub arrival_unix: i64,
    pub priority_seq: i64,
}

impl NewOrderRow {
    /// Rejects a note that is not a valid PSWAP order.
    pub fn ingested(note: &Note, arrival_unix: u64) -> DbResult<Self> {
        Order::from_note(note)?;
        Self::parsed(note, arrival_unix)
    }

    /// For a note the caller already parsed as a valid PSWAP order.
    pub fn parsed(note: &Note, arrival_unix: u64) -> DbResult<Self> {
        Ok(Self {
            note_id: note.id().to_bytes().to_vec(),
            raw_data: note.to_bytes(),
            arrival_unix: i64::try_from(arrival_unix)?,
        })
    }
}

impl NewRemainderOrderRow {
    /// Rejects a child that is not a valid PSWAP order of the same creator.
    pub fn from_parent(parent: &OrderRow, child: &Note) -> DbResult<Self> {
        let child_terms = Order::from_note(child)?;
        let parent_terms = Order::from_note(&parent.note()?)?;
        if child_terms.creator_id != parent_terms.creator_id {
            return Err(SettlementError::InvalidRemainder.into());
        }
        if parent.priority_seq <= 0 {
            return Err(DbError::Corrupt("parent order lacks a FIFO priority"));
        }
        Ok(Self {
            note_id: child.id().to_bytes().to_vec(),
            raw_data: child.to_bytes(),
            arrival_unix: parent.arrival_unix,
            priority_seq: parent.priority_seq,
        })
    }
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = settlement_attempts)]
pub struct SettlementAttemptRow {
    pub tx_id: Vec<u8>,
    pub tx_result: Vec<u8>,
    pub status: String,
}

impl SettlementAttemptRow {
    pub fn prepared(result: &TransactionResult) -> Self {
        Self {
            tx_id: result.id().to_bytes(),
            tx_result: result.to_bytes(),
            status: SettlementStatus::Prepared.as_str().to_owned(),
        }
    }

    pub fn settlement_status(&self) -> DbResult<SettlementStatus> {
        self.status
            .parse()
            .map_err(|_| DbError::Corrupt("unknown stored settlement status"))
    }
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = settlement_inputs)]
pub struct SettlementInputRow {
    pub tx_id: Vec<u8>,
    pub parent_note_id: Vec<u8>,
    pub child_note_id: Option<Vec<u8>>,
    pub child_note_data: Option<Vec<u8>>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = registered_tokens)]
pub struct RegisteredTokenRow {
    pub token_id: Vec<u8>,
    pub external_symbol: Option<String>,
    pub decimals: Option<i32>,
    pub ticker: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::OrderStatus;
    use anyhow::Result;
    use miden_protocol::asset::{AssetAmount, FungibleAsset};
    use miden_protocol::crypto::rand::{FeltRng, RandomCoin};
    use miden_protocol::note::NoteType;
    use miden_protocol::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
    };
    use miden_protocol::Word;
    use miden_standards::note::{PswapNote, PswapNoteStorage};

    #[test]
    fn stored_order_round_trips_and_rejects_a_mismatched_id() -> Result<()> {
        let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into()?;
        let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into()?;
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE.try_into()?;
        let sender = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2.try_into()?;
        let mut rng = RandomCoin::new(Word::default());
        let mut make = || -> Result<Note> {
            Ok(PswapNote::builder()
                .sender(sender)
                .storage(
                    PswapNoteStorage::builder()
                        .min_requested_asset(FungibleAsset::new(requested, 10)?)
                        .min_fill_step(AssetAmount::new(1)?)
                        .creator_account_id(creator)
                        .build(),
                )
                .serial_number(rng.draw_word())
                .note_type(NoteType::Public)
                .offered_asset(FungibleAsset::new(offered, 10)?)
                .build()?
                .into())
        };
        let note = make()?;
        let other = make()?;

        let new_order = NewOrderRow::ingested(&note, 123)?;
        assert_eq!(new_order.note_id, note.id().to_bytes());
        let row = OrderRow {
            note_id: new_order.note_id,
            raw_data: new_order.raw_data,
            arrival_unix: new_order.arrival_unix,
            status: OrderStatus::Active.as_str().into(),
            priority_seq: 1,
        };
        assert_eq!(row.clone().into_book_order()?.id(), note.id());
        let mut wrong_note = row;
        wrong_note.raw_data = other.to_bytes();
        assert!(wrong_note.into_book_order().is_err());
        Ok(())
    }
}
