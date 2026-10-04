//! Market-maker gateway (ADR 0003): authenticated gRPC commands on their own
//! thread, written by the maker intake on its own database session; the
//! maker-note watcher that makes submitted notes Live; and each maker's event
//! feed streamed from durable rows.

mod config;
mod error;
mod events;
mod intake;
mod service;
mod watcher;

pub use crate::maker::EventWake;
pub use config::{GatewayConfig, StreamConfig};
pub use error::IntakeError;
pub use events::append_event_tx;
pub use intake::{run_intake, IntakeReceiver, IntakeSender};
pub use service::{spawn_gateway_thread, MakerGatewayService};
pub use watcher::{run_watcher, MakerChain, RpcChain, WatchError, Watcher};

/// Generated from `proto/maker/v1/gateway.proto`.
pub mod proto {
    tonic::include_proto!("solver.maker.v1");
}
