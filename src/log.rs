//! The segment log: object keys and the pure rules that decide
//! what is valid to replay and which keys a new writer must occupy to fence an old one.
//!
//! Object layout under a disk prefix `P`:
//!   `P/epoch/<e:010>`          empty marker, created (create-only) by the writer that owns epoch `e`
//!   `P/seg/<e:010>/<s:012>`    segment `s` of epoch `e`, or the 4-byte tombstone `TOMB`
//!
//!   `P/ckpt/<e:010>/<s:012>`   checkpoint of the extent map after segment `s` of epoch `e`
//!
//! Segment objects are described in `segment.rs`, checkpoints in `extmap.rs`.

use anyhow::{bail, Result};
use std::collections::BTreeSet;
use std::os::unix::io::RawFd;

pub const TOMB: &[u8] = b"TOMB";

pub fn key_epoch(prefix: &str, epoch: u64) -> String {
    format!("{prefix}/epoch/{epoch:010}")
}
pub fn key_seg(prefix: &str, epoch: u64, seq: u64) -> String {
    format!("{prefix}/seg/{epoch:010}/{seq:012}")
}
/// `(epoch, seq)` of a segment key, `None` for anything else.
pub fn parse_seg_key(key: &str) -> Option<(u64, u64)> {
    let mut it = key.rsplit('/');
    let seq = it.next()?.parse().ok()?;
    let epoch = it.next()?.parse().ok()?;
    (it.next()? == "seg").then_some((epoch, seq))
}
pub fn parse_epoch_key(key: &str) -> Option<u64> {
    let mut it = key.rsplit('/');
    let e = it.next()?.parse().ok()?;
    (it.next()? == "epoch").then_some(e)
}

pub fn punch(fd: RawFd, off: u64, len: u64) -> Result<()> {
    let r = unsafe { libc::fallocate(fd, libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE, off as i64, len as i64) };
    if r != 0 {
        bail!("fallocate(PUNCH_HOLE): {}", std::io::Error::last_os_error());
    }
    Ok(())
}

pub fn is_tomb(obj: &[u8]) -> bool {
    obj == TOMB
}
pub fn key_ckpt(prefix: &str, epoch: u64, seq: u64) -> String {
    format!("{prefix}/ckpt/{epoch:010}/{seq:012}")
}
/// `(epoch, seq)` of a checkpoint key.
pub fn parse_ckpt_key(key: &str) -> Option<(u64, u64)> {
    let mut it = key.rsplit('/');
    let seq = it.next()?.parse().ok()?;
    let epoch = it.next()?.parse().ok()?;
    (it.next()? == "ckpt").then_some((epoch, seq))
}

/// How many of `seqs` (sorted) form the run `start, start + 1, ...`. Replay of an epoch takes this
/// run, by key presence, and additionally stops at the first tombstone, which is only known after
/// fetching. Below a checkpoint, GC may have removed dead segments, so the run starts after it.
pub fn contiguous_from(seqs: &[u64], start: u64) -> usize {
    seqs.iter().enumerate().take_while(|(i, s)| **s == start + *i as u64).count()
}

/// Keys a new writer must occupy (with tombstones) in an older epoch so that every PUT its writer
/// may still have in flight collides: every free seq from the first gap at or after `from` up to
/// max + span. An old writer keeps at most `window` PUTs in flight beyond its contiguous prefix, so
/// `span >= window` covers them. `from` is one past the epoch's newest checkpoint: holes below it
/// were made by GC, not by uploads in flight, and need no tombstones.
pub fn fence_targets(existing: &BTreeSet<u64>, span: u64, from: u64) -> Vec<u64> {
    let first_gap = (from..).find(|i| !existing.contains(i)).unwrap();
    let max = existing.iter().next_back().copied().unwrap_or(0).max(from);
    (first_gap..=max + span).filter(|s| !existing.contains(s)).collect()
}

/// An epoch is sealed once its highest object is a tombstone: a later writer fenced it, and the
/// old writer can no longer land anything (its window collides with the tombstones).
/// `objs`: seq -> is tombstone.
pub fn is_sealed(objs: &std::collections::BTreeMap<u64, bool>) -> bool {
    objs.values().next_back() == Some(&true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip() {
        assert_eq!(parse_seg_key(&key_seg("a/b", 3, 42)), Some((3, 42)));
        assert_eq!(parse_epoch_key(&key_epoch("a/b", 9)), Some(9));
        assert_eq!(parse_seg_key(&key_epoch("a", 9)), None);
        assert_eq!(parse_seg_key("a/seg/x/1"), None);
        // zero-padded keys sort numerically
        assert!(key_seg("p", 1, 9) < key_seg("p", 1, 10));
        assert!(key_seg("p", 9, 999) < key_seg("p", 10, 0));
        assert_eq!(parse_ckpt_key(&key_ckpt("a", 4, 12)), Some((4, 12)));
        assert_eq!(parse_ckpt_key(&key_seg("a", 4, 12)), None);
        assert!(is_tomb(TOMB) && !is_tomb(b"BGS2"));
    }

    #[test]
    fn prefix_stops_at_first_gap() {
        assert_eq!(contiguous_from(&[], 0), 0);
        assert_eq!(contiguous_from(&[0, 1, 2], 0), 3);
        assert_eq!(contiguous_from(&[0, 1, 3, 4], 0), 2);
        assert_eq!(contiguous_from(&[1, 2], 0), 0);
        assert_eq!(contiguous_from(&[5, 6, 8], 5), 2);
    }

    #[test]
    fn fence_covers_gaps_and_in_flight_window() {
        let set = |v: &[u64]| v.iter().copied().collect::<BTreeSet<u64>>();
        // nothing written yet: occupy 0..=span
        assert_eq!(fence_targets(&set(&[]), 3, 0), vec![0, 1, 2, 3]);
        // contiguous 0..=4: occupy 5..=4+span
        assert_eq!(fence_targets(&set(&[0, 1, 2, 3, 4]), 2, 0), vec![5, 6]);
        // out-of-order uploads left gaps at 2 and 4: fill them and the window past the max
        assert_eq!(fence_targets(&set(&[0, 1, 3, 5]), 2, 0), vec![2, 4, 6, 7]);
        // GC deleted 0..=6 below a checkpoint at 7: no tombstones in those holes
        assert_eq!(fence_targets(&set(&[7, 8, 9]), 2, 8), vec![10, 11]);
        // everything at or below the checkpoint was collected
        assert_eq!(fence_targets(&set(&[]), 2, 8), vec![8, 9, 10]);
        assert_eq!(fence_targets(&set(&[3]), 2, 8), vec![8, 9, 10]);
    }

    #[test]
    fn sealed_means_the_highest_object_is_a_tombstone() {
        let m = |v: &[(u64, bool)]| v.iter().copied().collect::<std::collections::BTreeMap<u64, bool>>();
        assert!(!is_sealed(&m(&[])));
        assert!(!is_sealed(&m(&[(0, false), (1, false)])));
        assert!(is_sealed(&m(&[(0, false), (1, true), (2, true)])));
        assert!(!is_sealed(&m(&[(0, true), (5, false)])), "a segment above every tombstone: not sealed");
    }
}
