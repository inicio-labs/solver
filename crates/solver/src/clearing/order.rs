use std::num::NonZeroU64;
use std::ops::RangeInclusive;

use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::note::{Note, NoteId};
use miden_standards::note::PswapNote;
use ruint::aliases::U256;

use crate::types::{FilledNote, IngestOrder};

use super::config::PPM_DENOMINATOR;
use super::math::{checked_mul, mul_div_ceil, mul_div_floor, ppm_floor, to_asset_amount};
use super::types::{BatchPrice, ClearingError, InvalidOrderReason, OrderExecution};

#[derive(Clone, Debug)]
pub struct Order {
    note_id: NoteId,
    note: PswapNote,
    priority: NonZeroU64,
}

impl Order {
    pub fn from_note(note: &Note, priority_sequence: u64) -> Result<Self, ClearingError> {
        let priority = NonZeroU64::new(priority_sequence).ok_or(ClearingError::InvalidOrder {
            note_id: note.id(),
            reason: InvalidOrderReason::MissingPriority,
        })?;
        let parsed = PswapNote::try_from(note).map_err(|source| ClearingError::InvalidPswap {
            note_id: note.id(),
            source,
        })?;
        Ok(Self {
            note_id: note.id(),
            note: parsed,
            priority,
        })
    }

    /// Parse the authoritative note bytes and verify every persisted cache field.
    pub fn from_ingest_order(order: &IngestOrder) -> Result<Self, ClearingError> {
        let note = Note::read_from(&mut SliceReader::new(&order.raw_note_data)).map_err(|_| {
            ClearingError::InvalidOrder {
                note_id: order.note_id,
                reason: InvalidOrderReason::MalformedRawNote,
            }
        })?;
        if note.id() != order.note_id {
            return Err(ClearingError::InvalidOrder {
                note_id: order.note_id,
                reason: InvalidOrderReason::InconsistentIngestOrder,
            });
        }

        let admitted = Self::from_note(&note, order.priority_seq)?;
        admitted.verify_ingest_fields(order)?;
        Ok(admitted)
    }

    fn verify_ingest_fields(&self, order: &IngestOrder) -> Result<(), ClearingError> {
        let offered = self.offered_asset();
        let requested = self.requested_asset();
        if offered.faucet_id() != order.offered_token
            || offered.amount().as_u64() != order.offered_amount
            || requested.faucet_id() != order.requested_token
            || requested.amount().as_u64() != order.requested_amount
            || self.note.storage().min_fill_step().as_u64() != order.min_fill_step
        {
            return Err(ClearingError::InvalidOrder {
                note_id: order.note_id,
                reason: InvalidOrderReason::InconsistentIngestOrder,
            });
        }
        Ok(())
    }

    pub fn id(&self) -> NoteId {
        self.note_id
    }

    pub fn priority_sequence(&self) -> u64 {
        self.priority.get()
    }

    pub fn pswap_note(&self) -> &PswapNote {
        &self.note
    }

    pub fn offered_asset(&self) -> FungibleAsset {
        *self.note.offered_asset()
    }

    pub fn requested_asset(&self) -> FungibleAsset {
        *self.note.storage().min_requested_asset()
    }

    pub(crate) fn prepare_if_eligible(
        &self,
        side: OrderSide,
        price: BatchPrice,
        fee_ppm: u32,
    ) -> Result<Option<MatchOrder<'_>>, ClearingError> {
        let offered = self.offered_asset();
        let requested = self.requested_asset();
        let (comparison_units, payment_units) = match side {
            OrderSide::SellBase => (
                checked_mul(price.quote_units, offered.amount().as_u64())?,
                checked_mul(price.base_units, requested.amount().as_u64())?,
            ),
            OrderSide::BuyBase => (
                checked_mul(price.base_units, offered.amount().as_u64())?,
                checked_mul(price.quote_units, requested.amount().as_u64())?,
            ),
        };
        let eligible = match side {
            OrderSide::SellBase => {
                checked_mul(payment_units, PPM_DENOMINATOR)?
                    <= checked_mul(comparison_units, PPM_DENOMINATOR - fee_ppm)?
            }
            OrderSide::BuyBase => {
                checked_mul(comparison_units, PPM_DENOMINATOR)?
                    >= checked_mul(payment_units, PPM_DENOMINATOR + fee_ppm)?
            }
        };
        if !eligible {
            return Ok(None);
        }

        let fill_scale = FillScale::new(comparison_units, payment_units);
        let minimum_payment = self.note.storage().min_fill_step().min(requested.amount());
        let minimum = fill_scale.to_comparison_floor(minimum_payment)?;
        let maximum = fill_scale.to_comparison_floor(requested.amount())?;

        Ok(Some(MatchOrder {
            order: self,
            side,
            fill_scale,
            comparison_fills: FillInterval { minimum, maximum },
        }))
    }

    /// Carry the verified fill and original note bytes to the executor.
    pub(crate) fn to_filled_note(
        &self,
        execution: &OrderExecution,
        arrival_unix: u64,
    ) -> FilledNote {
        let note: Note = self.note.clone().into();
        let mut raw_note_data = Vec::new();
        note.write_into(&mut raw_note_data);
        FilledNote {
            note_id: self.note_id,
            priority_seq: self.priority_sequence(),
            requested_filled: execution.payment.amount().as_u64(),
            raw_note_data,
            arrival_unix,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum OrderSide {
    SellBase,
    BuyBase,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FillInterval {
    pub minimum: U256,
    pub maximum: U256,
}

impl From<RangeInclusive<u128>> for FillInterval {
    fn from(range: RangeInclusive<u128>) -> Self {
        Self {
            minimum: U256::from(*range.start()),
            maximum: U256::from(*range.end()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FillScale {
    comparison_units: U256,
    payment_units: U256,
}

impl FillScale {
    fn new(comparison_units: U256, payment_units: U256) -> Self {
        let common = comparison_units.gcd(payment_units);
        Self {
            comparison_units: comparison_units / common,
            payment_units: payment_units / common,
        }
    }

    fn to_comparison_floor(self, payment: AssetAmount) -> Result<U256, ClearingError> {
        mul_div_floor(payment.as_u64(), self.comparison_units, self.payment_units)
    }

    fn to_payment_ceil(self, comparison: U256) -> Result<AssetAmount, ClearingError> {
        to_asset_amount(mul_div_ceil(
            comparison,
            self.payment_units,
            self.comparison_units,
        )?)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MatchOrder<'a> {
    order: &'a Order,
    side: OrderSide,
    fill_scale: FillScale,
    comparison_fills: FillInterval,
}

impl MatchOrder<'_> {
    pub(crate) fn order(&self) -> &Order {
        self.order
    }

    pub(crate) fn offered_amount(&self) -> AssetAmount {
        self.order.offered_asset().amount()
    }

    pub(crate) fn side(&self) -> OrderSide {
        self.side
    }

    pub(crate) fn fill_interval(&self) -> FillInterval {
        self.comparison_fills
    }

    pub(crate) fn execution(
        &self,
        comparison_fill: U256,
        fee_ppm: u32,
    ) -> Result<Option<OrderExecution>, ClearingError> {
        if comparison_fill == U256::ZERO {
            return Ok(None);
        }

        let offered = self.order.offered_asset();
        let requested = self.order.requested_asset();
        let (payment_amount, protocol_fee_target) =
            if comparison_fill == self.comparison_fills.maximum {
                let surplus = comparison_fill - U256::from(requested.amount().as_u64());
                let fee = surplus.min(ppm_floor(comparison_fill, fee_ppm)?);
                (to_asset_amount(comparison_fill - fee)?, fee)
            } else {
                (
                    self.fill_scale.to_payment_ceil(comparison_fill)?,
                    U256::ZERO,
                )
            };
        let release_amount = self
            .order
            .note
            .calculate_offered_for_requested(payment_amount.as_u64())
            .map_err(|_| ClearingError::InternalInvariant("protocol payout calculation failed"))?;
        let payment = FungibleAsset::new(requested.faucet_id(), payment_amount.as_u64())?;
        let release = FungibleAsset::new(offered.faucet_id(), release_amount)?;
        Ok(Some(OrderExecution {
            order_id: self.order.id(),
            comparison_fill,
            payment,
            release,
            protocol_fee_target,
        }))
    }
}
