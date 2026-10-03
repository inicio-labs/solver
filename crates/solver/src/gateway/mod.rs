//! Market-maker gateway (ADR 0003): authenticated gRPC commands on their own
//! thread, written by the maker intake on its own database session; the
//! maker-note watcher that makes submitted notes Live; and each maker's event
//! feed streamed from durable rows.

mod error;
mod events;
mod intake;
mod service;
mod watcher;

pub use crate::maker::EventWake;
pub use error::IntakeError;
pub use events::{append_event_tx, StreamConfig};
pub use intake::{intake_queues, run_intake, Intake, IntakeQueues};
pub use service::{spawn_gateway_thread, GatewayConfig, MakerGatewayService};
pub use watcher::{run_watcher, MakerChain, RpcChain, WatchError, Watcher};

/// Generated from `proto/maker/v1/gateway.proto`.
pub mod proto {
    tonic::include_proto!("solver.maker.v1");
}
