use std::cmp::Ordering;
use std::num::NonZeroU64;

use miden_protocol::account::AccountId;
use miden_protocol::asset::{AssetAmount, AssetId, FungibleAsset};
use miden_protocol::crypto::utils::{Deserializable, Serializable, SliceReader};
use miden_protocol::note::{Note, NoteId};
use miden_standards::note::PswapNote;
use ruint::aliases::U256;

use crate::types::{FilledNote, IngestOrder};

use super::math::{
    checked_mul, greatest_common_divisor, mul_div_ceil, mul_div_floor, ppm_floor, to_asset_amount,
};
use super::types::{
    ClearingError, ExactPrice, InvalidOrderReason, OrderExecution, PPM_DENOMINATOR,
};

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

    pub(crate) fn side_for(
        &self,
        base: AssetId,
        quote: AssetId,
    ) -> Result<OrderSide, ClearingError> {
        let offered = self.offered_asset().id();
        let requested = self.requested_asset().id();
        if offered == base && requested == quote {
            Ok(OrderSide::SellBase)
        } else if offered == quote && requested == base {
            Ok(OrderSide::BuyBase)
        } else {
            Err(ClearingError::InvalidOrder {
                note_id: self.id(),
                reason: InvalidOrderReason::UnsupportedAssetDirection,
            })
        }
    }

    pub(crate) fn is_eligible_at(
        &self,
        base: AssetId,
        quote: AssetId,
        price: ExactPrice,
        fee_ppm: u32,
    ) -> Result<bool, ClearingError> {
        if fee_ppm >= PPM_DENOMINATOR {
            return Err(ClearingError::InvalidConfig);
        }
        let side = self.side_for(base, quote)?;
        let offered = U256::from(self.offered_asset().amount().as_u64());
        let requested = U256::from(self.requested_asset().amount().as_u64());
        let fee_denominator = U256::from(PPM_DENOMINATOR);
        let fee = U256::from(fee_ppm);
        match side {
            OrderSide::SellBase => Ok(checked_mul(
                checked_mul(requested, price.base_units)?,
                fee_denominator,
            )? <= checked_mul(
                checked_mul(offered, price.quote_units)?,
                fee_denominator - fee,
            )?),
            OrderSide::BuyBase => Ok(checked_mul(
                checked_mul(offered, price.base_units)?,
                fee_denominator,
            )? >= checked_mul(
                checked_mul(requested, price.quote_units)?,
                fee_denominator + fee,
            )?),
        }
    }

    pub(crate) fn prepare(
        &self,
        base: AssetId,
        quote: AssetId,
        price: ExactPrice,
    ) -> Result<MatchOrder<'_>, ClearingError> {
        let offered = self.offered_asset();
        let requested = self.requested_asset();
        if offered.amount().as_u64() == 0 || requested.amount().as_u64() == 0 {
            return Err(ClearingError::InvalidOrder {
                note_id: self.id(),
                reason: InvalidOrderReason::ZeroAmount,
            });
        }

        let side = self.side_for(base, quote)?;
        let offered_amount = U256::from(offered.amount().as_u64());
        let requested_amount = U256::from(requested.amount().as_u64());
        let fill_scale = match side {
            OrderSide::SellBase => FillScale::new(
                checked_mul(price.quote_units, offered_amount)?,
                checked_mul(price.base_units, requested_amount)?,
            )?,
            OrderSide::BuyBase => FillScale::new(
                checked_mul(price.base_units, offered_amount)?,
                checked_mul(price.quote_units, requested_amount)?,
            )?,
        };
        let minimum_payment = self.minimum_fill()?;
        let minimum = fill_scale.to_comparison_floor(minimum_payment)?;
        let maximum = fill_scale.to_comparison_floor(requested.amount())?;
        if minimum > maximum
            || minimum < U256::from(minimum_payment.as_u64())
            || maximum < requested_amount
        {
            return Err(ClearingError::InternalInvariant(
                "invalid comparison fill range",
            ));
        }

        Ok(MatchOrder {
            order: self,
            side,
            minimum_payment,
            fill_scale,
            comparison_fills: FillInterval { minimum, maximum },
        })
    }

    fn minimum_fill(&self) -> Result<AssetAmount, ClearingError> {
        let requested = self.requested_asset().amount().as_u64();
        if requested == 0 {
            return Err(ClearingError::InvalidOrder {
                note_id: self.id(),
                reason: InvalidOrderReason::ZeroAmount,
            });
        }
        let configured = self.note.storage().min_fill_step().as_u64();
        Ok(AssetAmount::new(configured.min(requested).max(1))?)
    }

    /// Verify the protocol's actual payout before handing a fill to the executor.
    pub(crate) fn to_filled_note(
        &self,
        execution: &OrderExecution,
        solver_id: AccountId,
        arrival_unix: u64,
    ) -> Result<FilledNote, ClearingError> {
        if execution.order_id != self.note_id {
            return Err(ClearingError::InternalInvariant(
                "execution belongs to another order",
            ));
        }
        let (_, remainder) = self
            .note
            .execute(solver_id, None, Some(execution.payment))
            .map_err(|source| ClearingError::InvalidPswap {
                note_id: self.note_id,
                source,
            })?;
        let actual_release = match remainder {
            Some(remainder) => self
                .offered_asset()
                .amount()
                .as_u64()
                .checked_sub(remainder.offered_asset().amount().as_u64())
                .ok_or(ClearingError::InternalInvariant(
                    "remainder exceeds offered amount",
                ))?,
            None => self.offered_asset().amount().as_u64(),
        };
        if actual_release != execution.release.amount().as_u64() {
            return Err(ClearingError::InternalInvariant(
                "protocol release differs from plan",
            ));
        }

        let note: Note = self.note.clone().into();
        let mut raw_note_data = Vec::new();
        note.write_into(&mut raw_note_data);
        Ok(FilledNote {
            note_id: self.note_id,
            priority_seq: self.priority_sequence(),
            requested_filled: execution.payment.amount().as_u64(),
            raw_note_data,
            arrival_unix,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum OrderSide {
    SellBase,
    BuyBase,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FillInterval {
    pub minimum: U256,
    pub maximum: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FillScale {
    comparison_units: U256,
    payment_units: U256,
}

impl FillScale {
    fn new(comparison_units: U256, payment_units: U256) -> Result<Self, ClearingError> {
        if payment_units == U256::ZERO || comparison_units < payment_units {
            return Err(ClearingError::InternalInvariant("fill scale is below one"));
        }
        let common = greatest_common_divisor(comparison_units, payment_units);
        Ok(Self {
            comparison_units: comparison_units / common,
            payment_units: payment_units / common,
        })
    }

    fn to_comparison_floor(self, payment: AssetAmount) -> Result<U256, ClearingError> {
        mul_div_floor(
            U256::from(payment.as_u64()),
            self.comparison_units,
            self.payment_units,
        )
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
    minimum_payment: AssetAmount,
    fill_scale: FillScale,
    comparison_fills: FillInterval,
}

impl MatchOrder<'_> {
    pub(crate) fn side(&self) -> OrderSide {
        self.side
    }

    pub(crate) fn fill_interval(&self) -> FillInterval {
        self.comparison_fills
    }

    pub(crate) fn compare_price_time(&self, other: &Self) -> Ordering {
        debug_assert_eq!(self.side, other.side);
        let offered = u128::from(self.order.offered_asset().amount().as_u64());
        let requested = u128::from(self.order.requested_asset().amount().as_u64());
        let other_offered = u128::from(other.order.offered_asset().amount().as_u64());
        let other_requested = u128::from(other.order.requested_asset().amount().as_u64());
        let price_order = match self.side {
            OrderSide::SellBase => (requested * other_offered).cmp(&(other_requested * offered)),
            OrderSide::BuyBase => (other_offered * requested).cmp(&(offered * other_requested)),
        };
        price_order.then_with(|| {
            self.order
                .priority_sequence()
                .cmp(&other.order.priority_sequence())
        })
    }

    pub(crate) fn execution(
        &self,
        comparison_fill: U256,
        fee_ppm: u32,
    ) -> Result<Option<OrderExecution>, ClearingError> {
        if comparison_fill == U256::ZERO {
            return Ok(None);
        }
        if comparison_fill < self.comparison_fills.minimum
            || comparison_fill > self.comparison_fills.maximum
        {
            return Err(ClearingError::InternalInvariant(
                "allocation outside order fill range",
            ));
        }

        let (payment_amount, protocol_fee_target) =
            if comparison_fill == self.comparison_fills.maximum {
                let requested = U256::from(self.order.requested_asset().amount().as_u64());
                let surplus = comparison_fill.checked_sub(requested).ok_or(
                    ClearingError::InternalInvariant("full comparison fill below request"),
                )?;
                let fee = surplus.min(ppm_floor(comparison_fill, fee_ppm)?);
                (to_asset_amount(comparison_fill - fee)?, fee)
            } else {
                (
                    self.fill_scale.to_payment_ceil(comparison_fill)?,
                    U256::ZERO,
                )
            };
        if payment_amount < self.minimum_payment {
            return Err(ClearingError::InternalInvariant("payment below minimum"));
        }

        let release_amount = self
            .order
            .note
            .calculate_offered_for_requested(payment_amount.as_u64())
            .map_err(|_| ClearingError::InternalInvariant("protocol payout calculation failed"))?;
        let payment = FungibleAsset::new(
            self.order.requested_asset().faucet_id(),
            payment_amount.as_u64(),
        )?;
        let release = FungibleAsset::new(self.order.offered_asset().faucet_id(), release_amount)?;
        Ok(Some(OrderExecution {
            order_id: self.order.id(),
            comparison_fill,
            payment,
            release,
            protocol_fee_target,
        }))
    }
}
