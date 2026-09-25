use miden_protocol::asset::{AssetAmount, FungibleAsset};
use miden_standards::note::PswapNote;

use super::math::{asset_amount_from_wide, ppm_floor};
use super::types::{
    CandidatePlan, ClearingError, OrderExecution, PairAmounts, PreparedOrder, SettlementPlan, Side,
    SkipReason, SolverAccruals, Wide,
};

pub(crate) fn build_execution(
    order: &PreparedOrder,
    pswap: &PswapNote,
    scaled: Wide,
    fee_ppm: u32,
) -> Result<Option<OrderExecution>, ClearingError> {
    if scaled == Wide::ZERO {
        return Ok(None);
    }
    if scaled < order.domain.lo || scaled > order.domain.hi {
        return Err(ClearingError::InternalInvariant(
            "scaled allocation outside order domain",
        ));
    }

    let (payment_amount, nominal_fee) = if scaled == order.domain.hi {
        // Keep gross and fee wide until after subtraction. An endpoint gross
        // may exceed AssetAmount::MAX while the net payment remains valid.
        let requested = Wide::from(order.requested.amount().as_u64());
        let premium = scaled
            .checked_sub(requested)
            .ok_or(ClearingError::InternalInvariant("endpoint below request"))?;
        let fee = premium.min(ppm_floor(scaled, fee_ppm)?);
        (asset_amount_from_wide(scaled - fee)?, fee)
    } else {
        (order.scale.scale_down_ceil(scaled)?, Wide::ZERO)
    };
    if payment_amount < order.min_fill {
        return Err(ClearingError::InternalInvariant("payment below minimum"));
    }

    let release_amount = pswap
        .calculate_offered_for_requested(payment_amount.as_u64())
        .map_err(|_| ClearingError::InternalInvariant("protocol payout calculation failed"))?;
    let payment = FungibleAsset::new(order.requested.faucet_id(), payment_amount.as_u64())?;
    let release = FungibleAsset::new(order.offered.faucet_id(), release_amount)?;
    Ok(Some(OrderExecution {
        input_index: order.input_index,
        scaled,
        payment,
        release,
        nominal_protocol_fee: nominal_fee,
    }))
}

#[derive(Default)]
pub(crate) struct PairLedger {
    released: PairAmounts,
    paid: PairAmounts,
    nominal_fee: PairAmounts,
}

impl PairLedger {
    pub(crate) fn record(
        &mut self,
        side: Side,
        execution: &OrderExecution,
    ) -> Result<(), ClearingError> {
        let release = Wide::from(execution.release.amount().as_u64());
        let payment = Wide::from(execution.payment.amount().as_u64());
        let (released, paid, fee) = match side {
            Side::SellBase => (
                &mut self.released.base,
                &mut self.paid.quote,
                &mut self.nominal_fee.quote,
            ),
            Side::BuyBase => (
                &mut self.released.quote,
                &mut self.paid.base,
                &mut self.nominal_fee.base,
            ),
        };
        *released = released
            .checked_add(release)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        *paid = paid
            .checked_add(payment)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        *fee = fee
            .checked_add(execution.nominal_protocol_fee)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        Ok(())
    }

    pub(crate) fn into_totals(self) -> (PairAmounts, PairAmounts, PairAmounts) {
        (self.released, self.paid, self.nominal_fee)
    }
}

fn attribute_residual(released: Wide, paid: Wide, nominal_fee: Wide) -> Result<(Wide, Wide), Wide> {
    if released < paid {
        return Err(paid - released);
    }
    let residual = released - paid;
    let realized_fee = nominal_fee.min(residual);
    Ok((realized_fee, residual - realized_fee))
}

pub(crate) fn finalize_candidate(candidate: CandidatePlan) -> Result<SettlementPlan, SkipReason> {
    let base = attribute_residual(
        candidate.released.base,
        candidate.paid.base,
        candidate.nominal_protocol_fee.base,
    );
    let quote = attribute_residual(
        candidate.released.quote,
        candidate.paid.quote,
        candidate.nominal_protocol_fee.quote,
    );
    match (base, quote) {
        (Ok((fee_base, surplus_base)), Ok((fee_quote, surplus_quote))) => Ok(SettlementPlan {
            candidate,
            accruals: SolverAccruals {
                realized_protocol_fee: PairAmounts {
                    base: fee_base,
                    quote: fee_quote,
                },
                rounding_surplus: PairAmounts {
                    base: surplus_base,
                    quote: surplus_quote,
                },
            },
        }),
        (base, quote) => Err(SkipReason::Insolvent {
            base_shortfall: base.err().unwrap_or(Wide::ZERO),
            quote_shortfall: quote.err().unwrap_or(Wide::ZERO),
            candidate,
        }),
    }
}

pub(crate) fn effective_min_fill(pswap: &PswapNote) -> Result<AssetAmount, ClearingError> {
    let requested = pswap.storage().min_requested_asset().amount().as_u64();
    if requested == 0 {
        return Err(ClearingError::InternalInvariant("zero requested amount"));
    }
    let configured = pswap.storage().min_fill_step().as_u64();
    Ok(AssetAmount::new(configured.min(requested).max(1))?)
}
