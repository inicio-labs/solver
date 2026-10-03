//! Market-maker gateway (ADR 0003): authenticated gRPC commands on their own
//! thread, written by the maker intake on its own database session.

mod error;
mod intake;
mod service;

pub use error::IntakeError;
pub use intake::{intake_queues, run_intake, Intake, IntakeQueues};
pub use service::{spawn_gateway_thread, GatewayConfig, MakerGatewayService};

/// Generated from `proto/maker/v1/gateway.proto`.
pub mod proto {
    tonic::include_proto!("solver.maker.v1");
}
