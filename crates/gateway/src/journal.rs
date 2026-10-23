//! The journal of order ids: what the gateway has sent and does not know to have ended.
//!
//! The gateway writes the id of an order to disk **before** it sends the order to the broker. If the process dies after
//! the send and before the answer, the id is in the journal when the gateway comes back, and the first thing the new run
//! does is to compare the journal with the broker's active orders (reconcile) before it takes any command. The same
//! file tells which bot placed an order, which is what "my orders only" in `Events` filters by.
//!
//! The file is lines of JSON, appended and flushed with every change:
//!
//! ```text
//! {"op":"open","cid":"3f2a...","bot":"grid","at_ns":1792368000000000000}
//! {"op":"closed","cid":"3f2a..."}
//! ```
//!
//! At start the file is rewritten without the orders that are closed, so it does not grow from run to run.

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use brokapi::core::ClientOrderId;
use serde::{Deserialize, Serialize};

/// How many ended orders the journal remembers the bot of, in memory.
const REMEMBERED_ENDED: usize = 50_000;

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Line {
    Open {
        cid: ClientOrderId,
        bot: String,
        at_ns: i64,
    },
    Closed {
        cid: ClientOrderId,
    },
}

struct State {
    file: File,
    /// Orders not known to have ended: id -> bot.
    open: HashMap<ClientOrderId, String>,
    /// Orders that ended, the latest [`REMEMBERED_ENDED`], for the filter of events.
    ended: HashMap<ClientOrderId, String>,
    ended_order: VecDeque<ClientOrderId>,
}

/// The journal of the order ids of one gateway.
pub struct Journal {
    path: PathBuf,
    state: Mutex<State>,
}

impl Journal {
    /// Opens the journal in `dir` (made if it is not there), reads it and rewrites it without the closed orders. A
    /// last line cut short by a crash is ignored.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        let path = dir.join("journal.jsonl");
        let mut open: HashMap<ClientOrderId, (String, i64)> = HashMap::new();
        if let Ok(file) = File::open(&path) {
            for line in BufReader::new(file).lines() {
                let Ok(line) = line else { break };
                match serde_json::from_str::<Line>(&line) {
                    Ok(Line::Open { cid, bot, at_ns }) => {
                        open.insert(cid, (bot, at_ns));
                    }
                    Ok(Line::Closed { cid }) => {
                        open.remove(&cid);
                    }
                    // Half a line at the end after a crash, or a line of a newer version: skip it.
                    Err(_) => {}
                }
            }
        }
        // Rewrite what is still open, then append to it.
        let tmp = dir.join("journal.jsonl.tmp");
        {
            let mut out = File::create(&tmp)?;
            let mut lines: Vec<_> = open.iter().collect();
            lines.sort_by_key(|(_, (_, at))| *at);
            for (cid, (bot, at_ns)) in lines {
                let line = Line::Open {
                    cid: *cid,
                    bot: bot.clone(),
                    at_ns: *at_ns,
                };
                writeln!(
                    out,
                    "{}",
                    serde_json::to_string(&line).map_err(std::io::Error::other)?
                )?;
            }
            out.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        let file = OpenOptions::new().append(true).open(&path)?;
        Ok(Self {
            path,
            state: Mutex::new(State {
                file,
                open: open.into_iter().map(|(c, (b, _))| (c, b)).collect(),
                ended: HashMap::new(),
                ended_order: VecDeque::new(),
            }),
        })
    }

    /// Where the file is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn write(state: &mut State, line: &Line) -> std::io::Result<()> {
        let mut text = serde_json::to_string(line).map_err(std::io::Error::other)?;
        text.push('\n');
        state.file.write_all(text.as_bytes())?;
        // On disk before the order leaves: this is the point of the journal.
        state.file.sync_data()
    }

    /// Records that `bot` is about to send the order `cid`. Fails when it cannot be written: the order must then not be
    /// sent.
    pub fn open_order(&self, cid: ClientOrderId, bot: &str, at_ns: i64) -> std::io::Result<()> {
        let mut state = self.lock();
        if state.open.contains_key(&cid) {
            return Ok(());
        }
        Self::write(
            &mut state,
            &Line::Open {
                cid,
                bot: bot.to_owned(),
                at_ns,
            },
        )?;
        state.open.insert(cid, bot.to_owned());
        Ok(())
    }

    /// Records that the order has ended (or never was). A failure to write is not fatal: the next start finds the order
    /// closed at the broker and says so.
    pub fn close_order(&self, cid: ClientOrderId) {
        let mut state = self.lock();
        let Some(bot) = state.open.remove(&cid) else {
            return;
        };
        if let Err(e) = Self::write(&mut state, &Line::Closed { cid }) {
            tracing::warn!(%cid, "the journal could not record a closed order: {e}");
        }
        state.ended.insert(cid, bot);
        state.ended_order.push_back(cid);
        while state.ended_order.len() > REMEMBERED_ENDED {
            if let Some(old) = state.ended_order.pop_front() {
                state.ended.remove(&old);
            }
        }
    }

    /// The orders not known to have ended.
    pub fn open_orders(&self) -> Vec<ClientOrderId> {
        let mut cids: Vec<_> = self.lock().open.keys().copied().collect();
        cids.sort();
        cids
    }

    /// How many orders are not known to have ended.
    pub fn open_count(&self) -> usize {
        self.lock().open.len()
    }

    /// The bot that placed the order, if the journal knows it.
    pub fn bot_of(&self, cid: &ClientOrderId) -> Option<String> {
        let state = self.lock();
        state
            .open
            .get(cid)
            .or_else(|| state.ended.get(cid))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(n: u8) -> ClientOrderId {
        ClientOrderId::from_bytes([n; 16])
    }

    #[test]
    fn open_orders_survive_a_restart_closed_ones_do_not() {
        let dir = tempfile::tempdir().unwrap();
        {
            let j = Journal::open(dir.path()).unwrap();
            j.open_order(cid(1), "grid", 10).unwrap();
            j.open_order(cid(2), "scalper", 20).unwrap();
            j.open_order(cid(3), "grid", 30).unwrap();
            j.close_order(cid(2));
            assert_eq!(j.bot_of(&cid(2)).as_deref(), Some("scalper"));
        }
        let j = Journal::open(dir.path()).unwrap();
        assert_eq!(j.open_orders(), vec![cid(1), cid(3)]);
        assert_eq!(j.bot_of(&cid(1)).as_deref(), Some("grid"));
        // The file was rewritten without the ended order.
        let text = std::fs::read_to_string(j.path()).unwrap();
        assert_eq!(text.lines().count(), 2, "{text}");
    }

    #[test]
    fn a_line_cut_by_a_crash_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        {
            let j = Journal::open(dir.path()).unwrap();
            j.open_order(cid(1), "grid", 10).unwrap();
        }
        let path = dir.path().join("journal.jsonl");
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(b"{\"op\":\"open\",\"cid\":\"0202").unwrap();
        let j = Journal::open(dir.path()).unwrap();
        assert_eq!(j.open_orders(), vec![cid(1)]);
    }
}
