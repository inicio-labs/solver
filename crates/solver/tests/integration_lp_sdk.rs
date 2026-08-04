//! Seamless end-to-end: the **public `pswap-lp-sdk`** driving the **real router
//! thread** over a real websocket — the exact integration path an external DEX
//! (liquidity provider) follows. No internal solver types on the client side
//! beyond the shared protocol; everything goes through `LpClient`.
//!
//! Proves the turnkey flow end to end:
//!   1. wrong token → `connect` fails at the upgrade;
//!   2. right token → first event is `AuthOk`;
//!   3. a filler-centric `quote` → the (flipped) quote reaches the matcher's
//!      `quotes_rx`;
//!   4. a matcher `Handover` → arrives at the SDK as `LpEvent::Handover` carrying
//!      the decoded `Note` and the fill amount.
//!
//! The router-rejects-a-quote path is no longer reachable through the public SDK
//! (it validates amounts locally, and the pair travels as typed asset ids rather
//! than malformable hex), so `ServerMsg::Error → LpEvent::Error` surfacing is
//! covered by the SDK's own unit test (`client.rs`) instead of here.

use std::sync::Arc;
use std::time::Duration;

use miden_protocol::account::AccountId;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::Serializable;
use miden_protocol::note::Note;
use miden_protocol::testing::account_id::{
    ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
};
use miden_protocol::Word;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use pswap_lp_sdk::{LpClient, LpEvent};
use solver::router::{spawn_router_thread, RouteBatch, RoutedNote, QuotesSnapshot, RouterConfig};

fn free_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe); // release so the router thread can bind it
    port
}

/// Drive the router exclusively through the SDK's `LpClient`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sdk_lp_round_trip_against_real_router() {
    let port = free_port();

    // Matcher's ends of the two channels.
    let (quotes_tx, mut quotes_rx) = watch::channel::<Arc<QuotesSnapshot>>(Arc::new(std::collections::HashMap::new()));
    let (route_tx, route_rx) = mpsc::channel::<RouteBatch>(8);
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
        spawn_router_thread(cfg, quotes_tx, route_rx, cancel.clone()).unwrap();
    ready.await.unwrap().expect("router bound");

    let url = format!("ws://127.0.0.1:{port}/v1/rfq");

    // (1) Wrong token → upgrade rejected → connect errors.
    assert!(
        LpClient::connect(&url, "wrong-token").await.is_err(),
        "bad token must fail the connect"
    );

    // (2) Right token → first event is AuthOk.
    let mut client = LpClient::connect(&url, "dex-secret")
        .await
        .expect("authed connect");
    assert!(
        matches!(client.next_event().await, Some(LpEvent::AuthOk)),
        "first event after connect is AuthOk"
    );

    // (3) Post a filler-centric quote through the SDK: the DEX GIVES `b` and WANTS
    // `a`, so the router flips it to the note-centric pair (a, b).
    let a: AccountId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET.try_into().unwrap();
    let b: AccountId = ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1.try_into().unwrap();
    client
        .quote(
            FungibleAsset::new(b, 1_000).unwrap(), // offered = what the DEX gives
            FungibleAsset::new(a, 500).unwrap(),   // requested = what the DEX wants
            None,
        )
        .unwrap();

    // The (flipped) quote reaches the matcher's quotes_rx.
    tokio::time::timeout(Duration::from_secs(3), quotes_rx.changed())
        .await
        .expect("quote propagated to matcher")
        .unwrap();
    let snap = quotes_rx.borrow_and_update().clone();
    assert_eq!(snap.values().flatten().count(), 1, "exactly one standing quote");
    let q = snap.values().flatten().next().unwrap();
    assert_eq!(q.pair, (a, b), "pair flips to note orientation");
    assert_eq!((q.supply, q.demand), (1_000, 500), "the DEX's two base-unit amounts");
    let dex = q.dex;

    // (4) Matcher hands a real note over for that DEX → SDK surfaces the decoded
    // note + fill amount.
    let note = Note::mock_noop(Word::from([0xDEAD_BEEFu32, 4, 5, 6]));
    route_tx
        .send(RouteBatch {
            items: vec![RoutedNote {
                dex,
                note_id: note.id(),
                fill: 250,
                pair: (a, b),
                note_bytes: note.to_bytes(),
            }],
        })
        .await
        .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(3), client.next_event())
        .await
        .expect("handover delivered")
        .expect("event present");
    match ev {
        LpEvent::Handover(h) => {
            assert_eq!(h.fill_amount, 250);
            assert_eq!(h.note.to_bytes(), note.to_bytes(), "exact note bytes, decoded round-trip");
        }
        other => panic!("expected Handover, got {other:?}"),
    }

    // Graceful shutdown: drop client + handover sender, cancel, join the thread.
    drop(client);
    drop(route_tx);
    cancel.cancel();
    tokio::task::spawn_blocking(move || thread.join().unwrap())
        .await
        .unwrap();
}
