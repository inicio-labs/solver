//! Validated rows for the PostgreSQL application database.
//!
//! Ordinary orders intentionally have no `priority_seq` insertion field: the
//! database assigns it. Only a settlement remainder may inherit a priority.

use anyhow::{ensure, Result};
use diesel::prelude::*;
use miden_client::transaction::TransactionResult;
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::note::Note;
use thiserror::Error;

use super::postgres_schema::{
    notes, orders, registered_tokens, settlement_attempts, settlement_inputs, sync_state,
};
use crate::types::{BookOrder, Order, OrderId, OrderStatus};

#[derive(Debug, Error)]
pub enum StoredOrderError {
    #[error("stored order and note IDs differ")]
    NoteIdMismatch,
    #[error("stored order creator differs from note storage")]
    CreatorMismatch,
    #[error("stored order terms differ from note")]
    TermsMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettlementStatus {
    Prepared,
    Submitted,
    Uncertain,
    Rejected,
    Confirmed,
}

impl SettlementStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Submitted => "submitted",
            Self::Uncertain => "uncertain",
            Self::Rejected => "rejected",
            Self::Confirmed => "confirmed",
        }
    }
}

#[derive(Debug, Error)]
#[error("invalid stored settlement status: {0}")]
pub struct InvalidSettlementStatus(pub String);

impl TryFrom<&str> for SettlementStatus {
    type Error = InvalidSettlementStatus;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "prepared" => Ok(Self::Prepared),
            "submitted" => Ok(Self::Submitted),
            "uncertain" => Ok(Self::Uncertain),
            "rejected" => Ok(Self::Rejected),
            "confirmed" => Ok(Self::Confirmed),
            other => Err(InvalidSettlementStatus(other.to_owned())),
        }
    }
}

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = sync_state)]
pub struct SyncState {
    pub id: i16,
    pub last_fetched_block: i64,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = notes)]
pub struct NoteRow {
    pub note_id: Vec<u8>,
    pub account_id: Vec<u8>,
    pub raw_data: Vec<u8>,
}

#[derive(Queryable, Selectable, Debug, Clone)]
#[diesel(table_name = orders)]
pub struct OrderRow {
    pub note_id: Vec<u8>,
    pub account_id: Vec<u8>,
    pub requested_asset: Vec<u8>,
    pub requested_amount: i64,
    pub offered_asset: Vec<u8>,
    pub offered_amount: i64,
    pub arrival_unix: i64,
    pub status: String,
    pub priority_seq: i64,
}

impl OrderRow {
    pub fn order_status(&self) -> Result<OrderStatus> {
        OrderStatus::parse(&self.status)
            .ok_or_else(|| anyhow::anyhow!("invalid stored order status: {}", self.status))
    }

    pub fn into_book_order(self, raw_note_data: Vec<u8>) -> Result<BookOrder> {
        let note = Note::read_from(&mut SliceReader::new(&raw_note_data))?;
        let parsed = Order::from_note(&note)?;
        let note_id = OrderId::read_from(&mut SliceReader::new(&self.note_id))?;
        if note.id() != note_id {
            return Err(StoredOrderError::NoteIdMismatch.into());
        }
        if parsed.creator_id.to_bytes() != self.account_id {
            return Err(StoredOrderError::CreatorMismatch.into());
        }
        let terms_match = parsed.offered_faucet_id.to_bytes() == self.offered_asset
            && parsed.requested_faucet_id.to_bytes() == self.requested_asset
            && parsed.offered_amount == u64::try_from(self.offered_amount)?
            && parsed.requested_amount == u64::try_from(self.requested_amount)?;
        if !terms_match {
            return Err(StoredOrderError::TermsMismatch.into());
        }
        let priority_seq = u64::try_from(self.priority_seq)?;
        ensure!(priority_seq > 0, "stored order lacks FIFO priority");
        Ok(BookOrder {
            priority_seq,
            arrival_unix: u64::try_from(self.arrival_unix)?,
            note: std::sync::Arc::new(note),
        })
    }
}

/// A fresh external order. PostgreSQL's identity column is never supplied.
#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = orders)]
pub struct NewOrderRow {
    pub note_id: Vec<u8>,
    pub account_id: Vec<u8>,
    pub requested_asset: Vec<u8>,
    pub requested_amount: i64,
    pub offered_asset: Vec<u8>,
    pub offered_amount: i64,
    pub arrival_unix: i64,
}

/// A settlement child inherits the exact FIFO slot and arrival time of its
/// parent; it is the only kind of new order that supplies `priority_seq`.
#[derive(Insertable, Debug, Clone)]
#[diesel(table_name = orders)]
pub struct NewRemainderOrderRow {
    pub note_id: Vec<u8>,
    pub account_id: Vec<u8>,
    pub requested_asset: Vec<u8>,
    pub requested_amount: i64,
    pub offered_asset: Vec<u8>,
    pub offered_amount: i64,
    pub arrival_unix: i64,
    pub priority_seq: i64,
}

impl NewOrderRow {
    pub fn ingested(note: &Note, arrival_unix: u64) -> Result<(NoteRow, Self)> {
        let terms = Order::from_note(note)?;
        let account_id = terms.creator_id.to_bytes().to_vec();
        let note_id = note.id().to_bytes().to_vec();
        Ok((
            NoteRow {
                note_id: note_id.clone(),
                account_id: account_id.clone(),
                raw_data: note.to_bytes(),
            },
            Self {
                note_id,
                account_id,
                requested_asset: terms.requested_faucet_id.to_bytes().to_vec(),
                requested_amount: i64::try_from(terms.requested_amount)?,
                offered_asset: terms.offered_faucet_id.to_bytes().to_vec(),
                offered_amount: i64::try_from(terms.offered_amount)?,
                arrival_unix: i64::try_from(arrival_unix)?,
            },
        ))
    }
}

impl NewRemainderOrderRow {
    pub fn from_parent(parent: &OrderRow, child: &Note) -> Result<(NoteRow, Self)> {
        let terms = Order::from_note(child)?;
        let account_id = terms.creator_id.to_bytes().to_vec();
        let note_id = child.id().to_bytes().to_vec();
        ensure!(
            account_id == parent.account_id,
            "settlement remainder creator differs from parent"
        );
        ensure!(parent.priority_seq > 0, "parent lacks FIFO priority");
        ensure!(parent.arrival_unix >= 0, "parent has negative arrival time");
        Ok((
            NoteRow {
                note_id: note_id.clone(),
                account_id: account_id.clone(),
                raw_data: child.to_bytes(),
            },
            Self {
                note_id,
                account_id,
                requested_asset: terms.requested_faucet_id.to_bytes().to_vec(),
                requested_amount: i64::try_from(terms.requested_amount)?,
                offered_asset: terms.offered_faucet_id.to_bytes().to_vec(),
                offered_amount: i64::try_from(terms.offered_amount)?,
                arrival_unix: parent.arrival_unix,
                priority_seq: parent.priority_seq,
            },
        ))
    }
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = settlement_attempts)]
pub struct SettlementAttemptRow {
    pub tx_id: Vec<u8>,
    pub tx_result: Vec<u8>,
    pub status: String,
    pub created_at_unix: i64,
}

impl SettlementAttemptRow {
    pub fn prepared(result: &TransactionResult, created_at_unix: u64) -> Result<Self> {
        Ok(Self {
            tx_id: result.id().to_bytes(),
            tx_result: result.to_bytes(),
            status: SettlementStatus::Prepared.as_str().to_owned(),
            created_at_unix: i64::try_from(created_at_unix)?,
        })
    }

    pub fn settlement_status(&self) -> Result<SettlementStatus> {
        Ok(SettlementStatus::try_from(self.status.as_str())?)
    }
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = settlement_inputs)]
pub struct SettlementInputRow {
    pub tx_id: Vec<u8>,
    pub parent_note_id: Vec<u8>,
    pub payback_note_id: Vec<u8>,
    pub child_note_id: Option<Vec<u8>>,
    pub child_note_data: Option<Vec<u8>>,
}

#[derive(Queryable, Selectable, Insertable, Debug, Clone)]
#[diesel(table_name = registered_tokens)]
pub struct RegisteredTokenRow {
    pub token_id: Vec<u8>,
    pub created_at_unix: i64,
    pub external_symbol: Option<String>,
    pub decimals: Option<i32>,
    pub ticker: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn ingested_account_comes_from_note_creator_not_sender() -> Result<()> {
        let offered = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into()?;
        let requested = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into()?;
        let creator = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE.try_into()?;
        let sender = ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2.try_into()?;
        let mut rng = RandomCoin::new(Word::default());
        let note: Note = PswapNote::builder()
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
            .into();

        let (note_row, new_order) = NewOrderRow::ingested(&note, 123)?;
        assert_ne!(sender, creator);
        assert_eq!(note_row.account_id, creator.to_bytes());
        assert_eq!(new_order.account_id, creator.to_bytes());
        assert_eq!(note_row.note_id, note.id().to_bytes());
        let row = OrderRow {
            note_id: new_order.note_id,
            account_id: new_order.account_id,
            requested_asset: new_order.requested_asset,
            requested_amount: new_order.requested_amount,
            offered_asset: new_order.offered_asset,
            offered_amount: new_order.offered_amount,
            arrival_unix: new_order.arrival_unix,
            status: OrderStatus::Active.as_str().into(),
            priority_seq: 1,
        };
        row.clone().into_book_order(note_row.raw_data.clone())?;
        let mut wrong_creator = row;
        wrong_creator.account_id = sender.to_bytes().to_vec();
        assert!(wrong_creator.into_book_order(note_row.raw_data).is_err());
        Ok(())
    }
}
