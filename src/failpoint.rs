//! Failpoints for tests: named places in the code where a test can make the process exit, panic,
//! stall, or return an error.
//!
//! The set is read once per process from `BLOCKGW_FAILPOINTS` and from the file named by
//! `BLOCKGW_FAILPOINTS_FILE` (re-read by each new worker process, so a test can arm a failpoint
//! for the *next* restart). Format: `name=action[,name=action...]`, where action is one of
//! `exit`, `panic`, `error`, `sleep:<ms>`. With neither variable set, `hit` is a no-op.

use anyhow::{bail, Result};
use std::collections::HashMap;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Exit,
    Panic,
    Error,
    Sleep(u64),
}

pub fn parse(spec: &str) -> HashMap<String, Action> {
    spec.split([',', '\n'])
        .filter_map(|item| {
            let (name, action) = item.trim().split_once('=')?;
            if name.trim().is_empty() {
                return None;
            }
            let action = match action.trim() {
                "exit" => Action::Exit,
                "panic" => Action::Panic,
                "error" => Action::Error,
                a => Action::Sleep(a.strip_prefix("sleep:")?.parse().ok()?),
            };
            Some((name.trim().to_string(), action))
        })
        .collect()
}

fn armed() -> &'static HashMap<String, Action> {
    static SET: OnceLock<HashMap<String, Action>> = OnceLock::new();
    SET.get_or_init(|| {
        let mut spec = std::env::var("BLOCKGW_FAILPOINTS").unwrap_or_default();
        if let Ok(path) = std::env::var("BLOCKGW_FAILPOINTS_FILE") {
            spec.push(',');
            spec.push_str(&std::fs::read_to_string(path).unwrap_or_default());
        }
        parse(&spec)
    })
}

/// Trigger the failpoint `name` if it is armed.
pub fn hit(name: &str) -> Result<()> {
    match armed().get(name) {
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
        let m = parse("open=exit, upload = error,\nseal=sleep:250,bad,also=weird,x=sleep:nope,=exit");
        assert_eq!(m.get("open"), Some(&Action::Exit));
        assert_eq!(m.get("upload"), Some(&Action::Error));
        assert_eq!(m.get("seal"), Some(&Action::Sleep(250)));
        assert_eq!(m.len(), 3, "{m:?}");
        assert!(parse("").is_empty());
    }
}
