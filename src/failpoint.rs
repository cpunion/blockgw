//! Failpoints for tests: named places in the code where a test can make the process exit, panic,
//! stall, or return an error.
//!
//! The set is read once per process from `BLOCKGW_FAILPOINTS` and from the file named by
//! `BLOCKGW_FAILPOINTS_FILE` (re-read by each new worker process, so a test can arm a failpoint
//! for the *next* restart). Format: `name=action[%P][,name=action[%P]...]`, where action is one of
//! `exit`, `panic`, `error`, `sleep:<ms>`, and the optional `%P` makes it fire with probability P
//! percent (default 100). With neither variable set, `hit` is a no-op.

use anyhow::{bail, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Exit,
    Panic,
    Error,
    Sleep(u64),
}

pub fn parse(spec: &str) -> HashMap<String, (Action, u32)> {
    spec.split([',', '\n'])
        .filter_map(|item| {
            let (name, action) = item.trim().split_once('=')?;
            if name.trim().is_empty() {
                return None;
            }
            let (action, pct) = match action.trim().split_once('%') {
                Some((a, p)) => (a, p.trim().parse::<u32>().ok().filter(|p| *p <= 100)?),
                None => (action.trim(), 100),
            };
            let action = match action.trim() {
                "exit" => Action::Exit,
                "panic" => Action::Panic,
                "error" => Action::Error,
                a => Action::Sleep(a.strip_prefix("sleep:")?.parse().ok()?),
            };
            Some((name.trim().to_string(), (action, pct)))
        })
        .collect()
}

fn armed() -> &'static HashMap<String, (Action, u32)> {
    static SET: OnceLock<HashMap<String, (Action, u32)>> = OnceLock::new();
    SET.get_or_init(|| {
        let mut spec = std::env::var("BLOCKGW_FAILPOINTS").unwrap_or_default();
        if let Ok(path) = std::env::var("BLOCKGW_FAILPOINTS_FILE") {
            spec.push(',');
            spec.push_str(&std::fs::read_to_string(path).unwrap_or_default());
        }
        parse(&spec)
    })
}

/// xorshift64*, seeded per process; good enough to decide whether a failpoint fires
fn roll() -> u32 {
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE.load(Relaxed);
    if x == 0 {
        x = (std::process::id() as u64) << 32
            ^ std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
            | 1;
    }
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    STATE.store(x, Relaxed);
    (x.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 32) as u32 % 100
}

/// Trigger the failpoint `name` if it is armed (and its probability roll says so).
pub fn hit(name: &str) -> Result<()> {
    let action = match armed().get(name) {
        Some((a, pct)) if *pct >= 100 || roll() < *pct => Some(a),
        _ => None,
    };
    match action {
        None => Ok(()),
        Some(Action::Exit) => {
            eprintln!("failpoint {name}: exit");
            std::process::exit(101)
        }
        Some(Action::Panic) => panic!("failpoint {name}: panic"),
        Some(Action::Error) => bail!("failpoint {name}: injected error"),
        Some(Action::Sleep(ms)) => {
            std::thread::sleep(std::time::Duration::from_millis(*ms));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_actions_and_ignores_garbage() {
        let m = parse("open=exit, upload = error%30,\nseal=sleep:250,bad,also=weird,x=sleep:nope,=exit,y=error%101");
        assert_eq!(m.get("open"), Some(&(Action::Exit, 100)));
        assert_eq!(m.get("upload"), Some(&(Action::Error, 30)));
        assert_eq!(m.get("seal"), Some(&(Action::Sleep(250), 100)));
        assert_eq!(m.len(), 3, "{m:?}");
        assert!(parse("").is_empty());
    }

    #[test]
    fn rolls_are_roughly_uniform() {
        let n = 20_000;
        let under_30 = (0..n).filter(|_| roll() < 30).count();
        assert!((5_000..7_000).contains(&under_30), "{under_30}");
        assert!((0..1000).all(|_| roll() < 100));
    }
}
