use std::collections::HashMap;

use miden_protocol::account::AccountId;
use miden_protocol::note::NoteId;
use ruint::aliases::U256;

use crate::types::ExecutionBatch;

use super::matching::PairBatch;
use super::order::OrderSide;
use super::types::{
    CandidatePlan, ClearingError, OrderExecution, PairAmounts, SettlementPlan, SkipReason,
    SolverAccruals,
};

#[derive(Default)]
pub(crate) struct PairLedger {
    released: PairAmounts,
    paid: PairAmounts,
    protocol_fee_target: PairAmounts,
}

impl PairLedger {
    pub(crate) fn record(
        &mut self,
        side: OrderSide,
        execution: &OrderExecution,
    ) -> Result<(), ClearingError> {
        let released_amount = U256::from(execution.release.amount().as_u64());
        let paid_amount = U256::from(execution.payment.amount().as_u64());
        let (released, paid, fee_target) = match side {
            OrderSide::SellBase => (
                &mut self.released.base,
                &mut self.paid.quote,
                &mut self.protocol_fee_target.quote,
            ),
            OrderSide::BuyBase => (
                &mut self.released.quote,
                &mut self.paid.base,
                &mut self.protocol_fee_target.base,
            ),
        };
        *released = released
            .checked_add(released_amount)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        *paid = paid
            .checked_add(paid_amount)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        *fee_target = fee_target
            .checked_add(execution.protocol_fee_target)
            .ok_or(ClearingError::ArithmeticOverflow)?;
        Ok(())
    }

    pub(crate) fn totals(self) -> (PairAmounts, PairAmounts, PairAmounts) {
        (self.released, self.paid, self.protocol_fee_target)
    }
}

fn split_residual(released: U256, paid: U256, fee_target: U256) -> Result<(U256, U256), U256> {
    if released < paid {
        return Err(paid - released);
    }
    let residual = released - paid;
    let realized_fee = fee_target.min(residual);
    Ok((realized_fee, residual - realized_fee))
}

impl CandidatePlan {
    pub(crate) fn finalize(self) -> Result<SettlementPlan, SkipReason> {
        let base = split_residual(
            self.released.base,
            self.paid.base,
            self.protocol_fee_target.base,
        );
        let quote = split_residual(
            self.released.quote,
            self.paid.quote,
            self.protocol_fee_target.quote,
        );
        match (base, quote) {
            (Ok((fee_base, surplus_base)), Ok((fee_quote, surplus_quote))) => Ok(SettlementPlan {
                candidate: self,
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
                base_shortfall: base.err().unwrap_or(U256::ZERO),
                quote_shortfall: quote.err().unwrap_or(U256::ZERO),
                candidate: Box::new(self),
            }),
        }
    }
}

impl SettlementPlan {
    /// Recheck every planned output with the pinned protocol implementation and
    /// convert the plan into the executor's existing batch type.
    pub fn to_execution_batch(
        &self,
        batch: &PairBatch,
        solver_id: AccountId,
        arrival_unix: &HashMap<NoteId, u64>,
    ) -> Result<ExecutionBatch, ClearingError> {
        let mut filled_notes = Vec::with_capacity(self.candidate.executions.len());
        let orders_by_id: HashMap<_, _> = batch
            .orders()
            .iter()
            .map(|order| (order.id(), order))
            .collect();
        for execution in &self.candidate.executions {
            let order = orders_by_id.get(&execution.order_id).copied().ok_or(
                ClearingError::InternalInvariant("execution order is absent from batch"),
            )?;
            let arrival =
                arrival_unix
                    .get(&order.id())
                    .copied()
                    .ok_or(ClearingError::InternalInvariant(
                        "missing order arrival timestamp",
                    ))?;
            filled_notes.push(order.to_filled_note(execution, solver_id, arrival)?);
        }
        Ok(ExecutionBatch {
            filled_notes,
            group_ends: Vec::new(),
        })
    }
}
