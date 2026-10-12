//! The maker gateway gRPC client: TLS, the API key on every call, and one
//! method per RPC. See docs/market-maker-guide.md at the repository root for the semantics.

use std::time::Duration;

use anyhow::{Context, Result};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Request, Streaming};

pub mod pb {
    tonic::include_proto!("solver.maker.v1");
}

use pb::maker_gateway_client::MakerGatewayClient;

pub struct Gateway {
    client: MakerGatewayClient<Channel>,
    auth: MetadataValue<tonic::metadata::Ascii>,
}

impl Gateway {
    /// Open a channel. Use one channel for commands and reads and a separate
    /// one for the event stream, so a slow stream never delays a cancel.
    pub async fn connect(url: &str, api_key: &str) -> Result<Self> {
        let mut endpoint = Channel::from_shared(url.to_string())?
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true);
        if url.starts_with("https://") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
        }
        let channel = endpoint.connect().await.with_context(|| format!("connect to {url}"))?;
        let auth = format!("Bearer {api_key}").parse().context("API key must be ASCII")?;
        Ok(Self { client: MakerGatewayClient::new(channel), auth })
    }

    /// A unary request: API key plus a deadline.
    fn unary<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.metadata_mut().insert("authorization", self.auth.clone());
        request.set_timeout(Duration::from_secs(10));
        request
    }

    pub async fn submit_order(
        &mut self,
        request_id: &str,
        seq: u64,
        note: Vec<u8>,
        expires_at_unix_ms: Option<u64>,
    ) -> Result<pb::CommandReply> {
        let message = pb::SubmitOrderRequest { header: Some(header(request_id, seq)), note, expires_at_unix_ms };
        Ok(self.client.submit_order(self.unary(message)).await?.into_inner())
    }

    pub async fn cancel_order(&mut self, request_id: &str, seq: u64, lineage_id: Vec<u8>) -> Result<pb::CommandReply> {
        let message = pb::CancelOrderRequest { header: Some(header(request_id, seq)), lineage_id };
        Ok(self.client.cancel_order(self.unary(message)).await?.into_inner())
    }

    /// Cancel every older order, or only those in one market (two faucet ids, either order).
    pub async fn cancel_all(
        &mut self,
        request_id: &str,
        seq: u64,
        market: Option<(Vec<u8>, Vec<u8>)>,
    ) -> Result<pb::CommandReply> {
        let message = pb::CancelAllRequest {
            header: Some(header(request_id, seq)),
            order_type: pb::OrderType::Unspecified as i32,
            market: market.map(|(faucet_a, faucet_b)| pb::Market { faucet_a, faucet_b }),
            direction: None,
        };
        Ok(self.client.cancel_all(self.unary(message)).await?.into_inner())
    }

    pub async fn get_command(&mut self, request_id: &str) -> Result<pb::CommandReply> {
        let message = pb::GetCommandRequest { request_id: request_id.into() };
        Ok(self.client.get_command(self.unary(message)).await?.into_inner())
    }

    pub async fn list_active(&mut self) -> Result<Vec<pb::ActiveMakerOrder>> {
        let mut orders = Vec::new();
        let mut page_token = Vec::new();
        loop {
            let message = pb::ListActiveMakerOrdersRequest { page_token, page_size: 500 };
            let reply = self.client.list_active_maker_orders(self.unary(message)).await?.into_inner();
            orders.extend(reply.orders);
            if reply.next_page_token.is_empty() {
                return Ok(orders);
            }
            page_token = reply.next_page_token;
        }
    }

    pub async fn heartbeat(&mut self, received_through_seq: u64) -> Result<pb::HeartbeatReply> {
        let message = pb::HeartbeatRequest { received_through_seq };
        Ok(self.client.heartbeat(self.unary(message)).await?.into_inner())
    }

    /// The event stream: replay after `after_seq`, then live events. No deadline.
    pub async fn stream_events(&mut self, after_seq: u64) -> Result<Streaming<pb::StreamMessage>> {
        let mut request = Request::new(pb::StreamEventsRequest { after_seq });
        request.metadata_mut().insert("authorization", self.auth.clone());
        Ok(self.client.stream_events(request).await?.into_inner())
    }
}

fn header(request_id: &str, seq: u64) -> pb::CommandHeader {
    pb::CommandHeader { request_id: request_id.into(), seq }
}

/// One line per reply, for logs and the journal.
pub fn describe_reply(reply: &pb::CommandReply) -> String {
    use pb::command_reply::Result as R;
    let result = match &reply.result {
        Some(R::Accepted(_)) => "accepted".to_string(),
        Some(R::AlreadyRegistered(_)) => "already_registered".to_string(),
        Some(R::Applied(a)) => format!("applied cutoff={} settling={}", a.cutoff, a.settling),
        Some(R::Stopped(s)) => format!("stopped settling={}", s.settling),
        None => "empty".to_string(),
    };
    if reply.replayed { format!("{result} (replayed)") } else { result }
}
