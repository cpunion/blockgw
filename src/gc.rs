//! Garbage collection rules (pure): which objects may be deleted, which segments to repack.
//!
//! * A data segment may be deleted once it is at or below the position of the *older* of the last
//!   two checkpoints and neither of them references it. Keeping two checkpoints' worth of segments
//!   covers a reader that listed the older checkpoint just before the newer one was written.
//! * Checkpoints older than the older of the last two are deleted.
//! * Tombstones (4-byte objects) are never deleted: a fenced writer that is still running must keep
//!   colliding with them.
//! * A segment is repacked (its live blocks rewritten into the open segment) when less than
//!   `ratio` of its data is still live, oldest first, within a byte budget per round.

use crate::log;
use std::collections::{BTreeSet, HashMap};

pub struct Checkpoint {
    pub pos: (u64, u64),
    pub referenced: BTreeSet<(u64, u64)>,
}

/// Segment keys (from a sized listing) that may be deleted given the last two checkpoints.
pub fn deletable_segments(listing: &[(String, u64)], older: &Checkpoint, newer: &Checkpoint) -> Vec<String> {
    listing
        .iter()
        .filter(|(_, size)| *size as usize != log::TOMB.len())
        .filter_map(|(k, _)| log::parse_seg_key(k).map(|pos| (k, pos)))
        .filter(|(_, pos)| *pos <= older.pos && !older.referenced.contains(pos) && !newer.referenced.contains(pos))
        .map(|(k, _)| k.clone())
        .collect()
}

/// Checkpoint keys strictly older than `keep_from`.
pub fn deletable_checkpoints(listing: &[String], keep_from: (u64, u64)) -> Vec<String> {
    listing.iter().filter(|k| log::parse_ckpt_key(k).is_some_and(|pos| pos < keep_from)).cloned().collect()
}

/// Segments worth repacking: `(segment index, live blocks)`, oldest position first, while the live
/// blocks to rewrite stay within `budget_blocks`. `segs[i]` is `(epoch, seq)`, `data_blocks[i]` the
/// segment's data size; only segments at or below `upto` are considered.
pub fn repack_candidates(
    live: &HashMap<u32, u64>,
    segs: &[(u64, u64)],
    data_blocks: &[u32],
    upto: (u64, u64),
    ratio: f64,
    budget_blocks: u64,
) -> Vec<(u32, u64)> {
    let mut c: Vec<(u32, u64)> = live
        .iter()
        .filter(|(i, n)| {
            let i = **i as usize;
            let total = data_blocks[i] as u64;
            segs[i] <= upto && total > 0 && (**n as f64) < ratio * total as f64
        })
        .map(|(i, n)| (*i, *n))
        .collect();
    c.sort_by_key(|(i, _)| segs[*i as usize]);
    let mut used = 0;
    c.retain(|(_, n)| {
        let take = used + n <= budget_blocks;
        if take {
            used += n;
        }
        take
    });
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ck(pos: (u64, u64), refs: &[(u64, u64)]) -> Checkpoint {
        Checkpoint { pos, referenced: refs.iter().copied().collect() }
    }

    #[test]
    fn deletes_only_unreferenced_data_segments_below_the_older_checkpoint() {
        let k = |e, s| log::key_seg("d", e, s);
        let listing = vec![
            (k(1, 0), 1000), // dead, old: delete
            (k(1, 1), 1000), // referenced by older
            (k(1, 2), 1000), // referenced by newer only
            (k(1, 3), 4),    // tombstone: keep
            (k(2, 0), 1000), // above the older checkpoint: keep
            ("d/epoch/0000000002".into(), 0),
        ];
        let older = ck((1, 3), &[(1, 1)]);
        let newer = ck((2, 5), &[(1, 2), (2, 0)]);
        assert_eq!(deletable_segments(&listing, &older, &newer), vec![k(1, 0)]);
    }

    #[test]
    fn keeps_the_last_two_checkpoints() {
        let l = vec![log::key_ckpt("d", 1, 9), log::key_ckpt("d", 2, 3), log::key_ckpt("d", 2, 70), "d/seg/0000000002/000000000001".into()];
        assert_eq!(deletable_checkpoints(&l, (2, 3)), vec![log::key_ckpt("d", 1, 9)]);
    }

    #[test]
    fn repack_picks_sparse_old_segments_within_budget() {
        let segs = vec![(0, 0), (1, 0), (1, 1), (1, 2), (2, 0)];
        let data_blocks = vec![0, 100, 100, 100, 100];
        let live: HashMap<u32, u64> = [(1, 10), (2, 90), (3, 30), (4, 5)].into_iter().collect();
        // (2,0) is above `upto`; (1,1) is 90% live; budget fits (1,0) and (1,2)
        assert_eq!(repack_candidates(&live, &segs, &data_blocks, (1, 9), 0.5, 40), vec![(1, 10), (3, 30)]);
        assert_eq!(repack_candidates(&live, &segs, &data_blocks, (1, 9), 0.5, 20), vec![(1, 10)]);
    }
}
