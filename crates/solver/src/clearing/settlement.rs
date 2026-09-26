use std::collections::HashMap;

use miden_protocol::note::NoteId;

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
        let released_amount = u128::from(execution.release.amount().as_u64());
        let paid_amount = u128::from(execution.payment.amount().as_u64());
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
            .checked_add(
                u128::try_from(execution.protocol_fee_target)
                    .map_err(|_| ClearingError::ArithmeticOverflow)?,
            )
            .ok_or(ClearingError::ArithmeticOverflow)?;
        Ok(())
    }

    pub(crate) fn into_plan(self, executions: Vec<OrderExecution>) -> CandidatePlan {
        CandidatePlan {
            executions,
            released: self.released,
            paid: self.paid,
            protocol_fee_target: self.protocol_fee_target,
        }
    }
}

fn split_residual(released: u128, paid: u128, fee_target: u128) -> (u128, u128) {
    let residual = released - paid;
    let realized_fee = fee_target.min(residual);
    (realized_fee, residual - realized_fee)
}

impl CandidatePlan {
    pub(crate) fn finalize(self) -> Result<SettlementPlan, SkipReason> {
        let base_shortfall = self.paid.base.saturating_sub(self.released.base);
        let quote_shortfall = self.paid.quote.saturating_sub(self.released.quote);
        if base_shortfall > 0 || quote_shortfall > 0 {
            return Err(SkipReason::Insolvent {
                base_shortfall,
                quote_shortfall,
            });
        }
        let (fee_base, surplus_base) = split_residual(
            self.released.base,
            self.paid.base,
            self.protocol_fee_target.base,
        );
        let (fee_quote, surplus_quote) = split_residual(
            self.released.quote,
            self.paid.quote,
            self.protocol_fee_target.quote,
        );
        Ok(SettlementPlan {
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
        })
    }
}

impl SettlementPlan {
    /// Convert the solvent plan into the executor's existing batch type.
    pub fn to_execution_batch(
        &self,
        batch: &PairBatch<'_>,
        arrival_unix: &HashMap<NoteId, u64>,
    ) -> Result<ExecutionBatch, ClearingError> {
        let mut filled_notes = Vec::with_capacity(self.candidate.executions.len());
        // Matching emits executions in batch order, omitting skipped orders.
        // Walk that subsequence directly instead of indexing every note again.
        let mut executions = self.candidate.executions.iter();
        let mut next_execution = executions.next();
        for prepared in batch.orders() {
            let Some(execution) = next_execution else {
                break;
            };
            let order = prepared.order();
            if order.id() != execution.order_id {
                continue;
            }
            let arrival =
                arrival_unix
                    .get(&order.id())
                    .copied()
                    .ok_or(ClearingError::InternalInvariant(
                        "missing order arrival timestamp",
                    ))?;
            filled_notes.push(order.to_filled_note(execution, arrival));
            next_execution = executions.next();
        }
        if next_execution.is_some() {
            return Err(ClearingError::InternalInvariant(
                "execution order is absent from batch",
            ));
        }
        Ok(ExecutionBatch {
            filled_notes,
            group_ends: Vec::new(),
        })
    }
}
