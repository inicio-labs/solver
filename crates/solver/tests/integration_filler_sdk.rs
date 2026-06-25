//! Seamless end-to-end: the **public `pswap-filler-sdk`** driving the **real
//! router thread** over a real websocket — the exact integration path an
//! external DEX follows. No internal solver types on the client side beyond the
//! shared protocol; everything goes through `FillerClient`.
//!
//! Proves the turnkey flow end to end:
//!   1. wrong token → `connect` fails at the upgrade;
//!   2. right token → first event is `AuthOk`;
//!   3. `subscribe` + `quote` → the quote reaches the matcher's `quotes_rx`;
//!   4. a matcher `Handover` → arrives at the SDK as `FillerEvent::Handover`
//!      with the exact note bytes (hex) and fill amount.

use std::sync::Arc;
use std::time::Duration;

use miden_protocol::account::AccountId;
use miden_protocol::note::NoteId;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use pswap_filler_sdk::{FillerClient, FillerEvent, PairSpec};
use solver::router::{spawn_router_thread, Handover, HandoverPick, QuotesSnapshot, RouterConfig};

fn free_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe); // release so the router thread can bind it
    port
}

/// Drive the router exclusively through the SDK's `FillerClient`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_filler_round_trip_against_real_router() {
    let port = free_port();

    // Matcher's ends of the two channels.
    let (quotes_tx, mut quotes_rx) = watch::channel::<Arc<QuotesSnapshot>>(Arc::new(Vec::new()));
    let (handover_tx, handover_rx) = mpsc::channel::<Handover>(8);
    let cancel = CancellationToken::new();

    let cfg = RouterConfig {
        bind: "127.0.0.1".into(),
        port,
        max_connections: 8,
        max_msg_bytes: 16384,
        quote_ttl_ms: 20_000,
        auth_tokens: vec!["dex-secret".into()],
    };
    let (thread, ready) =
        spawn_router_thread(cfg, quotes_tx, handover_rx, cancel.clone()).unwrap();
    ready.await.unwrap().expect("router bound");

    let url = format!("ws://127.0.0.1:{port}/v1/rfq");

    // (1) Wrong token → upgrade rejected → connect errors.
    assert!(
        FillerClient::connect(&url, "wrong-token").await.is_err(),
        "bad token must fail the connect"
    );

    // (2) Right token → first event is AuthOk.
    let mut client = FillerClient::connect(&url, "dex-secret")
        .await
        .expect("authed connect");
    assert_eq!(
        client.next_event().await,
        Some(FillerEvent::AuthOk),
        "first event after connect is AuthOk"
    );

    // (3) Subscribe + post a standing quote through the SDK.
    let offered: AccountId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let requested: AccountId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
    let pair = PairSpec { offered: offered.to_hex(), requested: requested.to_hex() };

    client.subscribe(vec![pair.clone()]).unwrap();
    client.quote(&pair, "2.00", 1_000, None).unwrap();

    // The quote reaches the matcher's quotes_rx.
    tokio::time::timeout(Duration::from_secs(3), quotes_rx.changed())
        .await
        .expect("quote propagated to matcher")
        .unwrap();
    let snap = quotes_rx.borrow_and_update().clone();
    assert_eq!(snap.len(), 1, "exactly one standing quote");
    assert_eq!(snap[0].pair, (offered, requested));
    assert_eq!(snap[0].quantity, 1_000);
    let dex = snap[0].dex;

    // (4) Matcher hands a note over for that DEX → SDK surfaces it.
    let note_id = NoteId::try_from_hex(&format!("0x{:064x}", 7)).unwrap();
    handover_tx
        .send(Handover {
            items: vec![HandoverPick {
                dex,
                note_id,
                fill: 250,
                note_bytes: vec![0xDE, 0xAD, 0xBE, 0xEF],
                fill_price: "2.00".into(),
            }],
        })
        .await
        .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(3), client.next_event())
        .await
        .expect("handover delivered")
        .expect("event present");
    match ev {
        FillerEvent::Handover(h) => {
            assert_eq!(h.fill_amount, 250);
            assert_eq!(h.note_hex, "deadbeef");
            assert_eq!(h.note_id, note_id.to_string());
            assert_eq!(h.fill_price, "2.00");
        }
        other => panic!("expected Handover, got {other:?}"),
    }

    // Graceful shutdown: drop client + handover sender, cancel, join the thread.
    drop(client);
    drop(handover_tx);
    cancel.cancel();
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
}
