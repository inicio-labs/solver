//! Example market maker for the Inicio solver's maker gateway, Miden testnet.
//!
//! One command per step of a maker integration:
//!
//!   maker init                      create a wallet account and register it (the network sends USDCx for fees)
//!   maker address                   print the account id and testnet address
//!   maker claim                     consume incoming notes: faucet mints, USDCx, fill paybacks
//!   maker balance                   vault balance of every configured token
//!   maker quote --offer IETH:0.5 --request IUSDT:1250 [--expires-in 600] [--public]
//!                                   create a PSWAP order note and submit it to the gateway
//!   maker orders                    list your orders the solver can match now
//!   maker events [--follow]         replay your event feed from the saved cursor, then follow it
//!   maker cancel <lineage-hex>      cancel one order and its future remainders
//!   maker cancel-all [--market IETH,IUSDT]
//!                                   cancel every older order, or those in one market
//!   maker reclaim <lineage-hex>     take back an unfilled order's tokens (cancel first)
//!   maker heartbeat                 server time and your newest event sequence
//!   maker get-command <request-id>  the stored reply of a command you sent
//!
//! Config: ./maker.toml, or the path in MAKER_CONFIG. See maker-kit/README.md.

mod config;
mod gateway;
mod journal;
mod wallet;

use anyhow::{anyhow, bail, Context, Result};
use miden_protocol::account::AccountId;
use miden_protocol::address::Address;
use miden_protocol::asset::FungibleAsset;
use miden_protocol::crypto::utils::{Deserializable, Serializable};
use miden_protocol::note::Note;

use config::Config;
use gateway::{describe_reply, pb, Gateway};
use journal::{now_ms, Journal, OrderRecord};

fn main() -> Result<()> {
    // miden-client's Client is !Send: run everything on one thread.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(tokio::task::LocalSet::new().run_until(run()))
}

async fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str).unwrap_or("help");
    let path = std::env::var("MAKER_CONFIG").unwrap_or_else(|_| "maker.toml".into());
    if matches!(command, "help" | "-h" | "--help") {
        println!("{}", include_str!("main.rs").lines().take_while(|l| l.starts_with("//!")).map(|l| l.trim_start_matches("//!").strip_prefix(' ').unwrap_or("")).collect::<Vec<_>>().join("\n"));
        return Ok(());
    }
    let cfg = Config::load(&path)?;
    let mut journal = Journal::load(&cfg.state.journal)?;

    match command {
        "init" => init(&cfg, &mut journal).await,
        "address" => {
            let id = account(&journal)?;
            println!("account {}\naddress {}", id.to_hex(), Address::new(id).encode(cfg.network.address_network()?));
            Ok(())
        }
        "claim" => {
            let id = account(&journal)?;
            let (mut client, _) = wallet::client(&cfg).await?;
            let (claimed, skipped) = wallet::claim(&mut client, id).await?;
            println!("claimed {claimed} note(s), skipped {skipped}");
            Ok(())
        }
        "balance" => {
            let id = account(&journal)?;
            let (mut client, _) = wallet::client(&cfg).await?;
            client.sync_state().await?;
            for token in &cfg.tokens {
                let units = wallet::balance(&mut client, id, token.faucet_id()?).await?;
                println!("{:<7} {:>24}  ({units} base units)", token.symbol, token.to_whole(units));
            }
            Ok(())
        }
        "quote" => quote(&cfg, &mut journal, &args[1..]).await,
        "orders" => {
            let mut gw = connect(&cfg).await?;
            let now = now_ms();
            for o in gw.list_active().await? {
                let expired = o.expires_at_unix_ms.is_some_and(|e| e <= now);
                println!(
                    "{}  depth={} root_seq={} request={} expires={}{}",
                    hex::encode(&o.lineage_id), o.depth, o.root_seq, o.request_id,
                    o.expires_at_unix_ms.map(|e| e.to_string()).unwrap_or("never".into()),
                    if expired { " (expired: no longer matched)" } else { "" }
                );
            }
            Ok(())
        }
        "events" => events(&cfg, &mut journal, args.iter().any(|a| a == "--follow")).await,
        "cancel" => {
            let lineage = args.get(1).context("usage: maker cancel <lineage-hex>")?;
            let bytes = hex::decode(lineage).context("lineage must be hex")?;
            let (request_id, seq) = journal.begin("cancel", lineage.clone())?;
            let reply = connect(&cfg).await?.cancel_order(&request_id, seq, bytes).await?;
            let text = describe_reply(&reply);
            journal.finish(&request_id, text.clone())?;
            println!("{request_id}: {text}");
            if text.contains("settling=1") {
                println!("a settlement had already reserved it and may still fill; wait for its SettlementResolved before reclaiming");
            }
            Ok(())
        }
        "cancel-all" => {
            let market = match flag(&args, "--market") {
                Some(pair) => {
                    let (a, b) = pair.split_once(',').context("--market A,B")?;
                    Some((cfg.token(a)?.faucet_id()?.to_bytes(), cfg.token(b)?.faucet_id()?.to_bytes()))
                }
                None => None,
            };
            let (request_id, seq) = journal.begin("cancel-all", flag(&args, "--market").unwrap_or("all").to_string())?;
            let reply = connect(&cfg).await?.cancel_all(&request_id, seq, market).await?;
            let text = describe_reply(&reply);
            journal.finish(&request_id, text.clone())?;
            println!("{request_id}: {text}");
            Ok(())
        }
        "reclaim" => {
            let id = account(&journal)?;
            let lineage = args.get(1).context("usage: maker reclaim <lineage-hex>")?;
            let record = journal.order(lineage).context("no order with that lineage in the journal")?;
            let note = Note::read_from_bytes(&hex::decode(&record.note)?).map_err(|e| anyhow!("decode note: {e}"))?;
            let (mut client, _) = wallet::client(&cfg).await?;
            let depth = wallet::reclaim(&mut client, id, note).await?;
            println!("reclaimed the order's note at depth {depth}: its tokens are back in your account");
            Ok(())
        }
        "heartbeat" => {
            let reply = connect(&cfg).await?.heartbeat(journal.cursor).await?;
            let offset = reply.server_time_unix_ms - now_ms() as i64;
            println!("server time {} (offset {offset} ms), latest event seq {}, your cursor {}", reply.server_time_unix_ms, reply.latest_seq, journal.cursor);
            Ok(())
        }
        "get-command" => {
            let request_id = args.get(1).context("usage: maker get-command <request-id>")?;
            let reply = connect(&cfg).await?.get_command(request_id).await?;
            println!("{request_id}: {}", describe_reply(&reply));
            Ok(())
        }
        other => bail!("unknown command {other:?}; run `maker help`"),
    }
}

async fn init(cfg: &Config, journal: &mut Journal) -> Result<()> {
    if let Some(id) = &journal.account_id {
        bail!("this journal already has account {id}; use a new journal path for a new account");
    }
    wallet::ensure_parent(&cfg.state.journal)?;
    let (mut client, keystore) = wallet::client(cfg).await?;
    let id = wallet::create_account(&mut client, &keystore).await?;
    journal.account_id = Some(id.to_hex());
    journal.save()?;
    println!("account {}\naddress {}", id.to_hex(), Address::new(id).encode(cfg.network.address_network()?));
    wallet::register(cfg, &mut client, id).await?;
    println!("next: mint test tokens to this address (README), then `maker claim`");
    Ok(())
}

/// Create the order note on chain, record it, then register it with the gateway.
async fn quote(cfg: &Config, journal: &mut Journal, args: &[String]) -> Result<()> {
    let id = account(journal)?;
    let (offer_symbol, offer_amount) = flag(args, "--offer").and_then(|s| s.split_once(':')).context("--offer TOKEN:AMOUNT")?;
    let (want_symbol, want_amount) = flag(args, "--request").and_then(|s| s.split_once(':')).context("--request TOKEN:AMOUNT")?;
    let (offer_token, want_token) = (cfg.token(offer_symbol)?, cfg.token(want_symbol)?);
    let offered = FungibleAsset::new(offer_token.faucet_id()?, offer_token.to_base_units(offer_amount)?)?;
    let requested = FungibleAsset::new(want_token.faucet_id()?, want_token.to_base_units(want_amount)?)?;
    let expires_at_unix_ms = flag(args, "--expires-in")
        .map(|s| s.parse::<u64>().map(|secs| now_ms() + secs * 1000))
        .transpose()
        .context("--expires-in SECONDS")?;
    let public_note = args.iter().any(|a| a == "--public");

    let (mut client, _) = wallet::client(cfg).await?;
    let note = wallet::create_order(&mut client, id, offered, requested, public_note).await?;
    let lineage = wallet::lineage_id(&note)?;
    let lineage_hex = hex::encode(&lineage);

    // Rule 1: store the command (and the note, needed later to reclaim or to
    // rebuild paybacks) before sending it. A lost reply is retried unchanged.
    let detail = format!("{offer_amount} {} for {want_amount} {}", offer_token.symbol, want_token.symbol);
    let (request_id, seq) = journal.begin("submit", detail.clone())?;
    journal.orders.push(OrderRecord {
        lineage: lineage_hex.clone(),
        request_id: request_id.clone(),
        seq,
        note: hex::encode(note.to_bytes()),
        offered: format!("{offer_amount} {}", offer_token.symbol),
        requested: format!("{want_amount} {}", want_token.symbol),
        expires_at_unix_ms,
    });
    journal.save()?;

    let reply = connect(cfg).await?.submit_order(&request_id, seq, note.to_bytes(), expires_at_unix_ms).await?;
    let text = describe_reply(&reply);
    journal.finish(&request_id, text.clone())?;
    println!("{request_id}: {text}\n  {detail}\n  lineage {lineage_hex}\n  note    {}", note.id().to_hex());
    println!("accepted is not live yet: `maker events` shows OrderStatus LIVE once the solver sees the note on chain");
    Ok(())
}

/// Rule 2: apply each event and store its sequence as the cursor, then
/// reconnect from the cursor after any disconnect. Nothing is lost.
async fn events(cfg: &Config, journal: &mut Journal, follow: bool) -> Result<()> {
    loop {
        let mut gw = connect(cfg).await?;
        let mut stream = gw.stream_events(journal.cursor).await?;
        let result: Result<()> = async {
            while let Some(message) = stream.message().await? {
                match message.message {
                    Some(pb::stream_message::Message::Event(event)) => {
                        print_event(cfg, &event);
                        journal.cursor = event.seq;
                        journal.save()?;
                    }
                    Some(pb::stream_message::Message::ReplayComplete(r)) => {
                        println!("-- replay complete through #{}; live from here", r.through_seq);
                        if !follow {
                            return Ok(());
                        }
                    }
                    Some(pb::stream_message::Message::KeepAlive(_)) | None => {}
                }
            }
            Err(anyhow!("stream ended"))
        }
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(e) if follow => {
                eprintln!("stream: {e:#}; reconnecting from #{} in 2 s", journal.cursor);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

fn print_event(cfg: &Config, event: &pb::MakerEvent) {
    let lineage = hex::encode(&event.lineage_id);
    let short = &lineage[lineage.len().saturating_sub(16)..];
    let body = match event.body.as_ref().and_then(|b| b.kind.as_ref()) {
        Some(pb::event_body::Kind::OrderStatus(s)) => {
            let state = pb::OrderState::try_from(s.state).map(|s| s.as_str_name()).unwrap_or("?");
            format!("OrderStatus {state} depth={}{}", s.depth, if s.reason.is_empty() { String::new() } else { format!(" reason={:?}", s.reason) })
        }
        Some(pb::event_body::Kind::SettlementPending(p)) => {
            format!("SettlementPending tx={} {}", hex::encode(&p.tx_id), describe_fill(cfg, p.fill.as_ref()))
        }
        Some(pb::event_body::Kind::SettlementResolved(r)) => {
            let status = r.result.as_ref()
                .and_then(|x| pb::ResultingStatus::try_from(x.status).ok())
                .map(|s| s.as_str_name())
                .unwrap_or("?");
            format!(
                "SettlementResolved committed={} block={} result={status} {}",
                r.committed, r.commit_block, describe_fill(cfg, r.fill.as_ref())
            )
        }
        None => "event without a body".to_string(),
    };
    println!("#{} …{short} {body}", event.seq);
}

fn describe_fill(_cfg: &Config, fill: Option<&pb::InputFill>) -> String {
    match fill {
        Some(f) => format!(
            "paid_out={} received={} remaining_offered={} remaining_requested={}",
            f.offered_paid, f.payback_amount, f.remaining_offered, f.remaining_requested
        ),
        None => String::new(),
    }
}

async fn connect(cfg: &Config) -> Result<Gateway> {
    Gateway::connect(&cfg.gateway.url, &cfg.api_key()?).await
}

fn account(journal: &Journal) -> Result<AccountId> {
    let hex = journal.account_id.as_deref().context("no account yet: run `maker init`")?;
    AccountId::from_hex(hex).context("account id in the journal")
}

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).map(String::as_str)
}
