//! Optional threshold-only/exact-T policy. The legacy result is immutable.
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use super::*;
use crate::clearing::{CandidatePlan, OrderExecution, OrderSide, PairAmounts, SettlementPlan};

pub(super) struct Anchor {
    pub seller_allocations: Vec<U256>,
    pub buyer_allocations: Vec<U256>,
    pub candidate: CandidatePlan,
}

impl Anchor {
    /// Bound optional capture cost before cloning any vectors or records.
    pub(super) fn eligible(
        sellers: usize,
        buyers: usize,
        interval_cap: usize,
        candidate: &CandidatePlan,
    ) -> bool {
        let limits = WorkLimits::default();
        if sellers
            .checked_add(buyers)
            .is_none_or(|n| n > limits.orders)
            || interval_cap > limits.intervals
        {
            return false;
        }
        let base = candidate.paid.base > candidate.released.base;
        let quote = candidate.paid.quote > candidate.released.quote;
        match (base, quote) {
            (true, false) => sellers <= limits.donors,
            (false, true) => buyers <= limits.donors,
            _ => false,
        }
    }
}

/// Independent speculative-work limits, never limits on the legacy planner.
#[derive(Clone, Copy)]
struct WorkLimits {
    orders: usize,
    donors: usize,
    intervals: usize,
    candidates: usize,
    executions: usize,
    priority_visits: usize,
}
impl Default for WorkLimits {
    fn default() -> Self {
        Self {
            orders: 200,
            donors: 100,
            intervals: 50_000,
            candidates: 100,
            executions: 20_000,
            priority_visits: 10_000,
        }
    }
}

pub(super) struct Repair<'a, 'order> {
    pub batch: &'a PairBatch<'order>,
    pub config: &'a ClearingConfig,
    pub sells: &'a [MatchOrder<'order>],
    pub buys: &'a [MatchOrder<'order>],
    pub seller_fills: &'a ReachableFillMap,
    pub buyer_fills: &'a ReachableFillMap,
    pub anchor: &'a Anchor,
    pub legacy: &'a ClearingOutcome,
}

struct RankedPlan {
    plan: SettlementPlan,
    buyer_total: U256,
    deviation: U256,
    donor_index: usize,
    donor_comparison: U256,
}
impl RankedPlan {
    fn better_than(&self, other: &Self) -> bool {
        let payments = |p: &SettlementPlan| {
            p.candidate
                .executions
                .iter()
                .map(|e| e.payment.amount().as_u64())
                .collect::<Vec<_>>()
        };
        let releases = |p: &SettlementPlan| {
            p.candidate
                .executions
                .iter()
                .map(|e| e.release.amount().as_u64())
                .collect::<Vec<_>>()
        };
        self.buyer_total
            .cmp(&other.buyer_total)
            .then_with(|| other.deviation.cmp(&self.deviation))
            .then_with(|| payments(&self.plan).cmp(&payments(&other.plan)))
            .then_with(|| releases(&self.plan).cmp(&releases(&other.plan)))
            .then_with(|| other.donor_index.cmp(&self.donor_index))
            .then_with(|| other.donor_comparison.cmp(&self.donor_comparison))
            .is_gt()
    }
}

fn sum(values: &[U256]) -> Option<U256> {
    values
        .iter()
        .try_fold(U256::ZERO, |sum, &v| sum.checked_add(v))
}
fn complete(execution: &OrderExecution, order: &MatchOrder<'_>) -> bool {
    execution.payment.amount() >= order.order().requested_asset().amount()
}

impl Repair<'_, '_> {
    pub(super) fn run(&self) -> Option<SettlementPlan> {
        self.run_with(WorkLimits::default(), |plan| {
            let batch = plan.to_execution_batch(self.batch).ok()?;
            crate::executor::validate_completion_repair(batch).ok()
        })
    }

    /// The injected validator/work budget is used only by differential tests.
    fn run_with(
        &self,
        limits: WorkLimits,
        validate: impl FnOnce(&SettlementPlan) -> Option<()>,
    ) -> Option<SettlementPlan> {
        let started = Instant::now();
        let count = self.sells.len().checked_add(self.buys.len())?;
        if !self.config.integer_completion_repair
            || self.config.validate().is_err()
            || count > limits.orders
            || self.config.max_total_intervals > limits.intervals
            || self.anchor.seller_allocations.len() != self.sells.len()
            || self.anchor.buyer_allocations.len() != self.buys.len()
        {
            return None;
        }
        let anchor = &self.anchor.candidate;
        let base_deficit = anchor.paid.base.saturating_sub(anchor.released.base);
        let quote_deficit = anchor.paid.quote.saturating_sub(anchor.released.quote);
        let (donors, allocations, map, sell_donor, missing) = match (base_deficit, quote_deficit) {
            (d, 0) if d > 0 => (
                self.sells,
                &self.anchor.seller_allocations,
                self.seller_fills,
                true,
                d,
            ),
            (0, d) if d > 0 => (
                self.buys,
                &self.anchor.buyer_allocations,
                self.buyer_fills,
                false,
                d,
            ),
            _ => return None,
        };
        if donors.len() > limits.donors {
            return None;
        }
        let old = match self.legacy {
            ClearingOutcome::Accepted(plan) => Some(plan.as_ref()),
            ClearingOutcome::Skipped(_) => None,
        };
        let legacy_executions: HashMap<_, _> = old
            .into_iter()
            .flat_map(|p| &p.candidate.executions)
            .map(|e| (e.order_id, e))
            .collect();
        // Eligibility requires a completion in this exact first anchor which
        // the fallback did not achieve, before any donor search is attempted.
        if !self.sells.iter().chain(self.buys).any(|o| {
            let order_id = o.order().id();
            anchor
                .executions
                .iter()
                .find(|e| e.order_id == order_id)
                .is_some_and(|e| {
                    complete(e, o)
                        && legacy_executions
                            .get(&e.order_id)
                            .is_none_or(|old| !complete(old, o))
                })
        }) {
            return None;
        }
        // Invalid anchors are global failures. Regenerate once from the exact
        // source slices; do not infer skipped inputs from positional zipping.
        let regenerated = self.generate(
            &self.anchor.seller_allocations,
            &self.anchor.buyer_allocations,
            None,
            false,
        )?;
        if regenerated != *anchor {
            return None;
        }
        let mut tried = 0usize;
        // Reserve the regenerated anchor plus a conservative upper bound
        // of three source executions in each donor threshold calculation.
        // The final selected-winner executor preflight is separately bounded.
        let mut execution_checks = count;
        let mut priority_visits = 0usize;
        let mut winner: Option<RankedPlan> = None;
        for (index, order) in donors.iter().enumerate() {
            if allocations[index] == U256::ZERO {
                continue;
            }
            tried = tried.checked_add(1)?;
            execution_checks = execution_checks.checked_add(count.checked_add(3)?)?;
            priority_visits = priority_visits.checked_add(donors.len())?;
            if tried > limits.candidates
                || execution_checks > limits.executions
                || priority_visits > limits.priority_visits
            {
                tracing::debug!(
                    policy = "threshold-only-exact-t-v1",
                    tried,
                    reason = "work_cap",
                    "completion repair retained legacy result"
                );
                return None;
            }
            let Some(comparison) = order
                .release_threshold(allocations[index], missing, self.config.protocol_fee_ppm)
                .ok()
                .flatten()
            else {
                continue;
            };
            let mut changed = allocations.clone();
            changed[index] = comparison;
            let Some(target) = sum(&changed) else {
                continue;
            };
            // Larger totals can reallocate an earlier AON/minimum order.
            // Only the entire unchanged source priority vector is admissible.
            if map.allocate_by_priority(donors, target).ok().as_ref() != Some(&changed) {
                continue;
            }
            let (sellers, buyers) = if sell_donor {
                (&changed, &self.anchor.buyer_allocations)
            } else {
                (&self.anchor.seller_allocations, &changed)
            };
            let Some(candidate) = self.generate(sellers, buyers, Some(&legacy_executions), true)
            else {
                continue;
            };
            let Ok(plan) = candidate.finalize() else {
                continue;
            };
            if plan.accruals.realized_protocol_fee != plan.candidate.protocol_fee_target
                || old.is_some_and(|old| {
                    plan.accruals.realized_protocol_fee.base
                        < old.accruals.realized_protocol_fee.base
                        || plan.accruals.realized_protocol_fee.quote
                            < old.accruals.realized_protocol_fee.quote
                })
            {
                continue;
            }
            // Executor retains fees and rounding residual in ONE asset/token.
            if plan.candidate.released.base - plan.candidate.paid.base
                > u128::from(AssetAmount::MAX.as_u64())
                || plan.candidate.released.quote - plan.candidate.paid.quote
                    > u128::from(AssetAmount::MAX.as_u64())
            {
                continue;
            }
            let Some(buyer_total) = sum(buyers) else {
                continue;
            };
            let Some(seller_total) = sum(sellers) else {
                continue;
            };
            let Ok(coupled) = self.batch.clearing_price.quote_for_base_floor(buyer_total) else {
                continue;
            };
            let ranked = RankedPlan {
                plan,
                buyer_total,
                deviation: seller_total.max(coupled) - seller_total.min(coupled),
                donor_index: index,
                donor_comparison: comparison,
            };
            if winner.as_ref().is_none_or(|old| ranked.better_than(old)) {
                winner = Some(ranked);
            }
        }
        let Some(winner) = winner else {
            tracing::debug!(
                policy = "threshold-only-exact-t-v1",
                tried,
                reason = "no_repair_in_bounded_set",
                "completion repair retained legacy result"
            );
            return None;
        };
        // A failed winner falls back; it never triggers another search.
        if validate(&winner.plan).is_none() {
            tracing::debug!(
                policy = "threshold-only-exact-t-v1",
                tried,
                reason = "final_validation",
                "completion repair retained legacy result"
            );
            return None;
        }
        let zero = PairAmounts::default();
        let fallback_paid = old.map_or(&zero, |p| &p.candidate.paid);
        let fallback_released = old.map_or(&zero, |p| &p.candidate.released);
        tracing::info!(policy = "threshold-only-exact-t-v1", enabled = true,
            price = ?self.batch.clearing_price, tried, execution_checks, priority_visits,
            elapsed_us = started.elapsed().as_micros(),
            anchor_paid = ?anchor.paid, anchor_released = ?anchor.released,
            anchor_fee_target = ?anchor.protocol_fee_target,
            anchor_raw_base = anchor.released.base as i128 - anchor.paid.base as i128,
            anchor_raw_quote = anchor.released.quote as i128 - anchor.paid.quote as i128,
            fallback_paid = ?fallback_paid, fallback_released = ?fallback_released,
            fallback_fee_target = ?old.map(|p| &p.candidate.protocol_fee_target),
            fallback_accruals = ?old.map(|p| &p.accruals),
            candidate_paid = ?winner.plan.candidate.paid,
            candidate_released = ?winner.plan.candidate.released,
            candidate_fee_target = ?winner.plan.candidate.protocol_fee_target,
            candidate_accruals = ?winner.plan.accruals,
            paid_base_delta = winner.plan.candidate.paid.base as i128 - fallback_paid.base as i128,
            paid_quote_delta = winner.plan.candidate.paid.quote as i128 - fallback_paid.quote as i128,
            "bounded completion repair selected");
        Some(winner.plan)
    }

    /// Regenerate in canonical source order. Candidate checks are deliberately
    /// stronger than the legacy ledger, and never retroactively reject it.
    fn generate(
        &self,
        sellers: &[U256],
        buyers: &[U256],
        legacy: Option<&HashMap<miden_protocol::note::NoteId, &OrderExecution>>,
        validate_candidate: bool,
    ) -> Option<CandidatePlan> {
        let base = self.sells.first()?.order().offered_asset().faucet_id();
        let quote = self.sells.first()?.order().requested_asset().faucet_id();
        if base == quote {
            return None;
        }
        let mut ids = HashSet::new();
        let mut ledger = PairLedger::default();
        let mut executions = Vec::new();
        let mut extra_completion = false;
        let mut preserved = 0;
        for (orders, allocations) in [(self.sells, sellers), (self.buys, buyers)] {
            if orders.len() != allocations.len() {
                return None;
            }
            for (order, &comparison) in orders.iter().zip(allocations) {
                if !ids.insert(order.order().id()) {
                    return None;
                }
                let offered = order.order().offered_asset().faucet_id();
                let requested = order.order().requested_asset().faucet_id();
                let tokens = match order.side() {
                    OrderSide::SellBase => (base, quote),
                    OrderSide::BuyBase => (quote, base),
                };
                if (offered, requested) != tokens {
                    return None;
                }
                let interval = order.fill_interval();
                if comparison != U256::ZERO
                    && (comparison < interval.minimum || comparison > interval.maximum)
                {
                    return None;
                }
                let execution = order
                    .execution(comparison, self.config.protocol_fee_ppm)
                    .ok()?;
                let prior = legacy.and_then(|old| old.get(&order.order().id()));
                let Some(execution) = execution else {
                    if prior.is_some() {
                        return None;
                    }
                    continue;
                };
                if validate_candidate {
                    let payment = execution.payment.amount().as_u64();
                    let requested = order.order().requested_asset().amount().as_u64();
                    let minimum = order
                        .order()
                        .pswap_note()
                        .storage()
                        .min_fill_step()
                        .as_u64()
                        .min(requested);
                    if payment == 0
                        || payment < minimum
                        || (complete(&execution, order) && comparison != interval.maximum)
                    {
                        return None;
                    }
                    if let Some(prior) = prior {
                        preserved += 1;
                        if comparison < prior.comparison_fill
                            || execution.payment.amount() < prior.payment.amount()
                            || execution.release.amount() < prior.release.amount()
                            || execution.payment.faucet_id() != prior.payment.faucet_id()
                            || execution.release.faucet_id() != prior.release.faucet_id()
                        {
                            return None;
                        }
                    }
                    extra_completion |= complete(&execution, order)
                        && prior.is_none_or(|old| !complete(old, order));
                }
                ledger.record(order.side(), &execution).ok()?;
                executions.push(execution);
            }
        }
        if validate_candidate
            && (!extra_completion || legacy.is_some_and(|old| preserved != old.len()))
        {
            return None;
        }
        Some(ledger.into_plan(executions))
    }
}

#[cfg(test)]
mod tests;
