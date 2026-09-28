//! RFQ dispatch and temporary reservations, independent of the clearing algorithm.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::{Context, Result};
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::NoteId;
use tokio::sync::{mpsc, watch};

use super::{select_notes, QuotesSnapshot, RouteBatch, RoutedNote};
use crate::matcher::clearing_book::ClearingBook;

pub struct Routing {
    quotes: watch::Receiver<Arc<QuotesSnapshot>>,
    handovers: mpsc::Sender<RouteBatch>,
    reservations: VecDeque<(NoteId, u64)>,
    inflight_ttl_ms: u64,
}

impl Routing {
    pub fn new(
        quotes: watch::Receiver<Arc<QuotesSnapshot>>,
        handovers: mpsc::Sender<RouteBatch>,
        inflight_ttl_ms: u64,
    ) -> Self {
        Self {
            quotes,
            handovers,
            reservations: VecDeque::new(),
            inflight_ttl_ms,
        }
    }

    /// Preserve the existing RFQ timeout policy. Ingestion removes confirmed
    /// consumed notes; expired handovers return only notes still in the book.
    pub(crate) fn release_expired(&mut self, book: &mut ClearingBook, now: u64) -> Result<()> {
        // Inspect only the expired prefix, not every outstanding RFQ each tick.
        while let Some(&(id, sent)) = self.reservations.front() {
            if now.saturating_sub(sent) < self.inflight_ttl_ms {
                break;
            }
            book.reactivate(id)?;
            self.reservations.pop_front();
        }
        Ok(())
    }

    /// Only unmatched active notes enter RFQ. Reserve channel space before
    /// deactivating anything: backpressure leaves the book unchanged.
    pub(crate) fn dispatch(&mut self, book: &mut ClearingBook, now: u64) -> Result<()> {
        let quotes = self.quotes.borrow().clone();
        let picks = select_notes(&book.routing_orders(&quotes), &quotes, now);
        if picks.is_empty() {
            return Ok(());
        }
        let permit = match self.handovers.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => return Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("RFQ router stopped: handover channel closed");
            }
        };
        let items = picks
            .into_iter()
            .map(|pick| {
                let note = book.note(pick.note_id).context("missing routed note")?;
                Ok(RoutedNote {
                    dex: pick.dex,
                    note_id: pick.note_id,
                    fill: pick.fill,
                    pair: pick.pair,
                    note_bytes: note.to_bytes(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for item in &items {
            book.deactivate(item.note_id);
            self.reservations.push_back((item.note_id, now));
        }
        tracing::info!(count = items.len(), "routed unmatched notes to DEXes");
        permit.send(RouteBatch { items });
        Ok(())
    }
}
