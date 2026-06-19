//! Solver devnet end-to-end harness.
//!
//! Subcommands:
//!   * `provision` — create + deploy the solver wallet and two faucets on
//!     devnet; fund the solver's inventory buffer; write artifacts + solver.toml.
//!   * `fund`      — top up the solver (and consume P2ID notes targeting it).
//!   * `load`      — provision user wallets and create opposing PSWAP pairs.
//!   * `run`       — run the solver in-process vs devnet and assert settlement.
//!
//! Runs on a current-thread runtime + LocalSet because the miden `Client` is
//! `!Send` (matching the solver binary's own main).

mod accounts;
mod artifacts;
mod devnet;
mod ops;
mod provision;
mod run;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Parser)]
#[command(name = "e2e", about = "Solver devnet end-to-end harness")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create + deploy the solver wallet and two fungible faucets on devnet,
    /// fund the solver's buffer, and write artifacts + a devnet solver.toml.
    Provision,
    /// Top up the solver's inventory and consume any committed notes targeting
    /// it (the harness's mints AND any P2ID notes a user sent).
    Fund {
        /// Per-token amount to mint into the solver.
        #[arg(long, default_value_t = 50_000_000)]
        amount: u64,
    },
    /// Mint a token to an arbitrary account (e.g. fund a user's wallet so they
    /// can create their own PSWAPs in the hand-off flow).
    Mint {
        /// Target account id (hex).
        #[arg(long)]
        to: String,
        /// Which faucet: `a`/`mta` or `b`/`mtb`.
        #[arg(long)]
        token: String,
        /// Base-unit amount to mint.
        #[arg(long)]
        amount: u64,
    },
    /// Provision user wallets and create opposing PSWAP pairs (matchable orders).
    Load {
        /// How many opposing PSWAP pairs to create.
        #[arg(long, default_value_t = 1)]
        rounds: u32,
    },
    /// Run the solver in-process against devnet; reports the settlement summary.
    Run {
        /// How long to run before shutting down (seconds).
        #[arg(long, default_value_t = 240)]
        secs: u64,
    },
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,e2e=info,solver=info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().compact())
        .init();
}

fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        match cli.command {
            Command::Provision => provision::run().await,
            Command::Fund { amount } => ops::fund(amount).await,
            Command::Mint { to, token, amount } => ops::mint(&to, &token, amount).await,
            Command::Load { rounds } => ops::load(rounds).await,
            Command::Run { secs } => run::run(secs).await,
        }
    })
}
