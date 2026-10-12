//! The maker's durable state, in one JSON file.
//!
//! Two rules from the maker guide drive this file:
//! 1. Save each command (request id, sequence, payload) BEFORE sending it, so a
//!    lost reply can be retried with exactly the same command.
//! 2. Save the event cursor together with each event's effect, and reconnect
//!    the event stream from it.
//!
//! A production maker would use a database transaction for each of these;
//! a JSON file rewritten atomically is enough to show the flow.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Journal {
    #[serde(skip)]
    path: PathBuf,
    /// The wallet account that creates orders and receives paybacks.
    pub account_id: Option<String>,
    /// Next command sequence. One counter for everything the maker sends.
    pub next_seq: u64,
    /// Sequence of the last event applied.
    pub cursor: u64,
    pub commands: Vec<CommandRecord>,
    pub orders: Vec<OrderRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRecord {
    pub request_id: String,
    pub seq: u64,
    pub kind: String,
    pub detail: String,
    /// The gateway's reply once received; `None` means "resend me unchanged".
    pub reply: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRecord {
    /// 47-byte lineage id, hex. Cancel by this.
    pub lineage: String,
    pub request_id: String,
    pub seq: u64,
    /// The submitted note, canonical bytes in hex. Needed to reclaim the
    /// order and to rebuild payback and remainder notes after fills.
    pub note: String,
    pub offered: String,
    pub requested: String,
    pub expires_at_unix_ms: Option<u64>,
}

impl Journal {
    pub fn load(path: &Path) -> Result<Self> {
        let mut journal = if path.exists() {
            let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
            serde_json::from_str::<Journal>(&text).with_context(|| format!("parse {}", path.display()))?
        } else {
            // Start sequences from the clock so they never go backwards, even if
            // a fresh journal is created for the same maker later.
            Journal { next_seq: now_ms() * 1000, ..Default::default() }
        };
        journal.path = path.to_path_buf();
        Ok(journal)
    }

    /// Atomic rewrite: a crash leaves either the old or the new file.
    pub fn save(&self) -> Result<()> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Allocate a sequence and record the command, then persist, before sending.
    pub fn begin(&mut self, kind: &str, detail: String) -> Result<(String, u64)> {
        let seq = self.next_seq;
        self.next_seq += 1;
        let request_id = format!("{kind}-{seq}");
        self.commands.push(CommandRecord { request_id: request_id.clone(), seq, kind: kind.into(), detail, reply: None });
        self.save()?;
        Ok((request_id, seq))
    }

    pub fn finish(&mut self, request_id: &str, reply: String) -> Result<()> {
        if let Some(c) = self.commands.iter_mut().find(|c| c.request_id == request_id) {
            c.reply = Some(reply);
        }
        self.save()
    }

    pub fn order(&self, lineage_hex: &str) -> Option<&OrderRecord> {
        self.orders.iter().find(|o| o.lineage == lineage_hex)
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}
