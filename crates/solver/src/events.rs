use serde::Serialize;

use crate::order::Order;

#[derive(Debug, Clone, Serialize)]
pub struct OrderDto {
    pub id: String,
    pub rate: f64,
    pub qty: u64,
    pub time: u64,
    pub side: String,
    pub offered_amount: u64,
    pub requested_amount: u64,
}

impl OrderDto {
    pub fn from_order(order: &Order, side: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        OrderDto {
            id: order.note.id().to_string(),
            rate: order.price_ratio(),
            qty: order.offered_amount,
            time: now,
            side: side.to_string(),
            offered_amount: order.offered_amount,
            requested_amount: order.requested_amount,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MatchDto {
    pub ask_ids: Vec<String>,
    pub bid_ids: Vec<String>,
    pub surplus_x: u64,
    pub surplus_y: u64,
    pub total_x: u64,
    pub total_y: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum SolverEvent {
    OrderBookSnapshot {
        asks: Vec<OrderDto>,
        bids: Vec<OrderDto>,
    },
    MatchExecuted {
        matched: MatchDto,
    },
    MatchFailed {
        error: String,
    },
}
