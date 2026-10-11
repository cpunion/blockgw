//! Pure bookkeeping: write-intent regions and in-order commit of out-of-order uploads.

use std::collections::BTreeSet;

/// Write-intent tracking per fixed-size region.
///
/// A region's bit must be durable before the first write into it is acknowledged; the bit may be
/// cleared once the last segment holding a write into the region is committed to the object store
/// and no write into it is in flight. After a crash, the set bits name the regions whose contents
/// may be missing from the object store, so the dirty set stays close to the un-uploaded tail
/// instead of growing to "everything written since the disk was last idle".
pub struct RegionTracker {
    bits: Vec<u64>,
    last_seq: Vec<u64>, // seq+1 of the last segment holding a write into the region (0 = none)
    inflight: Vec<u32>,
}

impl RegionTracker {
    pub fn new(regions: usize, persisted: &[u8]) -> Self {
        let words = regions.div_ceil(64);
        let mut bits = vec![0u64; words];
        for (w, c) in persisted.chunks(8).take(words).enumerate() {
            let mut b = [0u8; 8];
            b[..c.len()].copy_from_slice(c);
            bits[w] = u64::from_le_bytes(b);
        }
        if regions % 64 != 0 {
            bits[words - 1] &= (1u64 << (regions % 64)) - 1;
        }
        RegionTracker { bits, last_seq: vec![0; regions], inflight: vec![0; regions] }
    }

    pub fn regions(&self) -> usize {
        self.last_seq.len()
    }

    /// A write into regions `a..=b` starts. Returns true if a bit was newly set, i.e. the caller must
    /// persist `to_bytes()` before letting the write proceed.
    pub fn begin(&mut self, a: u64, b: u64) -> bool {
        let mut changed = false;
        for r in a..=b {
            self.inflight[r as usize] += 1;
            let (w, m) = ((r / 64) as usize, 1u64 << (r % 64));
            if self.bits[w] & m == 0 {
                self.bits[w] |= m;
                changed = true;
            }
        }
        changed
    }

    /// The write finished; `seq` is the segment that received it (`None` if the write failed).
    pub fn end(&mut self, a: u64, b: u64, seq: Option<u64>) {
        for r in a..=b {
            let r = r as usize;
            self.inflight[r] -= 1;
            if let Some(s) = seq {
                self.last_seq[r] = self.last_seq[r].max(s + 1);
            }
        }
    }

    /// Clear the bits of regions whose last write is in a committed segment (`committed` = seq+1 of
    /// the highest committed segment) and that have no write in flight. Returns true if any cleared.
    pub fn clear_committed(&mut self, committed: u64) -> bool {
        let mut cleared = false;
        for w in 0..self.bits.len() {
            let mut word = self.bits[w];
            while word != 0 {
                let i = word.trailing_zeros() as usize;
                word &= word - 1;
                let r = w * 64 + i;
                if self.inflight[r] == 0 && self.last_seq[r] <= committed {
                    self.bits[w] &= !(1u64 << i);
                    cleared = true;
                }
            }
        }
        cleared
    }

    pub fn dirty(&self) -> Vec<u64> {
        (0..self.regions() as u64).filter(|r| self.bits[(r / 64) as usize] >> (r % 64) & 1 == 1).collect()
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.bits.iter().flat_map(|w| w.to_le_bytes()).collect()
    }
}

/// Uploads complete out of order; the object store must only ever be considered to hold a prefix of
/// the log. `complete(seq)` returns the seqs that became committed, in order.
#[derive(Default)]
pub struct CommitTracker {
    committed: u64, // seq+1 of the highest committed segment
    done: BTreeSet<u64>,
}

impl CommitTracker {
    pub fn committed(&self) -> u64 {
        self.committed
    }
    pub fn complete(&mut self, seq: u64) -> Vec<u64> {
        if seq >= self.committed {
            self.done.insert(seq);
        }
        let mut out = Vec::new();
        while self.done.remove(&self.committed) {
            out.push(self.committed);
            self.committed += 1;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_write_into_a_region_needs_persisting() {
        let mut t = RegionTracker::new(10, &[]);
        assert!(t.begin(2, 3));
        assert!(!t.begin(3, 3), "already dirty");
        assert_eq!(t.dirty(), vec![2, 3]);
    }

    #[test]
    fn clear_waits_for_commit_and_inflight() {
        let mut t = RegionTracker::new(4, &[]);
        t.begin(1, 1);
        assert!(!t.clear_committed(100), "write still in flight");
        t.end(1, 1, Some(5)); // landed in segment 5
        assert!(!t.clear_committed(5), "segment 5 not committed yet (committed = 0..=4)");
        assert!(t.clear_committed(6));
        assert!(t.dirty().is_empty());
        // re-dirtying a cleared region must be persisted again
        assert!(t.begin(1, 1));
    }

    #[test]
    fn failed_write_does_not_hold_a_region() {
        let mut t = RegionTracker::new(4, &[]);
        t.begin(0, 0);
        t.end(0, 0, None);
        assert!(t.clear_committed(0));
    }

    #[test]
    fn overlapping_writes_keep_the_latest_segment() {
        let mut t = RegionTracker::new(4, &[]);
        t.begin(2, 2);
        t.begin(2, 2);
        t.end(2, 2, Some(9));
        t.end(2, 2, Some(3));
        assert!(!t.clear_committed(4), "the later write (segment 9) is not committed");
        assert!(t.clear_committed(10));
    }

    #[test]
    fn bytes_round_trip_and_ignore_padding() {
        let mut t = RegionTracker::new(70, &[]);
        t.begin(0, 0);
        t.begin(69, 69);
        let b = t.to_bytes();
        assert_eq!(b.len(), 16);
        assert_eq!(RegionTracker::new(70, &b).dirty(), vec![0, 69]);
        // bits past the region count in a persisted file are ignored
        assert_eq!(RegionTracker::new(3, &[0xff]).dirty(), vec![0, 1, 2]);
        // a short or missing file means clean
        assert!(RegionTracker::new(70, &[]).dirty().is_empty());
    }

    #[test]
    fn commits_only_the_contiguous_prefix() {
        let mut c = CommitTracker::default();
        assert_eq!(c.complete(1), Vec::<u64>::new());
        assert_eq!(c.complete(2), Vec::<u64>::new());
        assert_eq!(c.committed(), 0);
        assert_eq!(c.complete(0), vec![0, 1, 2]);
        assert_eq!(c.committed(), 3);
        assert_eq!(c.complete(4), Vec::<u64>::new());
        assert_eq!(c.complete(3), vec![3, 4]);
        assert_eq!(c.complete(1), Vec::<u64>::new(), "duplicate completion is ignored");
        assert_eq!(c.committed(), 5);
    }
}
