//! Minimal leveled logging to stderr: `2026-10-11T13:00:00.123Z INFO message`.
//! The level comes from `BLOCKGW_LOG` (error, warn, info, debug; default info).

use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

impl Level {
    fn name(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
        }
    }
    pub fn parse(s: &str) -> Option<Level> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Some(Level::Error),
            "warn" | "warning" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" => Some(Level::Debug),
            _ => None,
        }
    }
}

pub fn max_level() -> Level {
    static L: OnceLock<Level> = OnceLock::new();
    *L.get_or_init(|| std::env::var("BLOCKGW_LOG").ok().and_then(|v| Level::parse(&v)).unwrap_or(Level::Info))
}

/// RFC 3339 UTC timestamp with milliseconds.
pub fn timestamp(t: SystemTime) -> String {
    let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // civil-from-days (Howard Hinnant's algorithm)
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z", rem / 3600, rem % 3600 / 60, rem % 60, d.subsec_millis())
}

pub fn emit(level: Level, msg: std::fmt::Arguments) {
    if level <= max_level() {
        eprintln!("{} {} {msg}", timestamp(SystemTime::now()), level.name());
    }
}

#[macro_export]
macro_rules! error { ($($t:tt)*) => { $crate::logx::emit($crate::logx::Level::Error, format_args!($($t)*)) } }
#[macro_export]
macro_rules! warn { ($($t:tt)*) => { $crate::logx::emit($crate::logx::Level::Warn, format_args!($($t)*)) } }
#[macro_export]
macro_rules! info { ($($t:tt)*) => { $crate::logx::emit($crate::logx::Level::Info, format_args!($($t)*)) } }
#[macro_export]
macro_rules! debug { ($($t:tt)*) => { $crate::logx::emit($crate::logx::Level::Debug, format_args!($($t)*)) } }

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn timestamps_are_rfc3339_utc() {
        assert_eq!(timestamp(UNIX_EPOCH), "1970-01-01T00:00:00.000Z");
        assert_eq!(timestamp(UNIX_EPOCH + Duration::from_millis(951_782_400_123)), "2000-02-29T00:00:00.123Z");
        assert_eq!(timestamp(UNIX_EPOCH + Duration::from_secs(1_791_694_861)), "2026-10-11T05:01:01.000Z");
        assert_eq!(timestamp(UNIX_EPOCH + Duration::from_secs(4_102_444_799)), "2099-12-31T23:59:59.000Z");
    }

    #[test]
    fn levels_parse_and_order() {
        assert_eq!(Level::parse("DEBUG"), Some(Level::Debug));
        assert_eq!(Level::parse("warning"), Some(Level::Warn));
        assert_eq!(Level::parse("loud"), None);
        assert!(Level::Error < Level::Info);
    }
}
