//! Which 4 KiB blocks of the local image hold valid data (lazy restore).
//!
//! After a lazy restore, the image starts empty and every block the extent map points at is
//! *absent*: its data lives only in the object store. A block becomes *present* when it is fetched
//! or written locally. Rules that keep the image correct:
//!
//! * a fetch may only fill a block that is still absent and not being written: a write's data is
//!   always newer than anything fetched from the committed log;
//! * a write marks its blocks as being written before touching the image, and present only after
//!   the image write completed;
//! * the persisted bitmap must never claim a block present whose data is not durable in the image:
//!   a flush takes a snapshot of the bitmap, then fdatasyncs the image, then persists the snapshot.
//!   A crash can only leave bits stale-absent, which costs a refetch, never stale-present.

use std::collections::HashMap;

pub struct Presence {
    bits: Vec<u64>, // 1 = present
    absent: u64,
    writing: HashMap<u64, u32>, // block -> writes in flight
}

impl Presence {
    /// All blocks present (a disk whose image is complete).
    pub fn full(blocks: u64) -> Self {
        let mut p = Presence { bits: vec![u64::MAX; blocks.div_ceil(64) as usize], absent: 0, writing: HashMap::new() };
        p.trim(blocks);
        p
    }

    /// From a persisted bitmap (missing bytes = absent).
    pub fn from_bytes(blocks: u64, bytes: &[u8]) -> Self {
        let words = blocks.div_ceil(64) as usize;
        let mut bits = vec![0u64; words];
        for (w, c) in bytes.chunks(8).take(words).enumerate() {
            let mut b = [0u8; 8];
            b[..c.len()].copy_from_slice(c);
            bits[w] = u64::from_le_bytes(b);
        }
        let mut p = Presence { bits, absent: 0, writing: HashMap::new() };
        p.trim(blocks);
        p.absent = blocks - p.bits.iter().map(|w| w.count_ones() as u64).sum::<u64>();
        p
    }

    fn trim(&mut self, blocks: u64) {
        if blocks % 64 != 0 {
            if let Some(last) = self.bits.last_mut() {
                *last &= (1u64 << (blocks % 64)) - 1;
            }
        }
    }

    pub fn is_present(&self, b: u64) -> bool {
        self.bits[(b / 64) as usize] >> (b % 64) & 1 == 1
    }

    fn set(&mut self, b: u64) {
        let (w, m) = ((b / 64) as usize, 1u64 << (b % 64));
        if self.bits[w] & m == 0 {
            self.bits[w] |= m;
            self.absent -= 1;
        }
    }

    pub fn absent(&self) -> u64 {
        self.absent
    }

    /// Absent blocks in `[b, b + n)`.
    #[cfg(test)]
    pub fn absent_in(&self, b: u64, n: u64) -> Vec<u64> {
        (b..b + n).filter(|x| !self.is_present(*x)).collect()
    }

    /// A write to `[b, b + n)` starts.
    pub fn write_begin(&mut self, b: u64, n: u64) {
        for x in b..b + n {
            *self.writing.entry(x).or_insert(0) += 1;
        }
    }

    /// The write finished (`ok`: its data is in the image, so the blocks are present).
    pub fn write_end(&mut self, b: u64, n: u64, ok: bool) {
        for x in b..b + n {
            if let Some(c) = self.writing.get_mut(&x) {
                *c -= 1;
                if *c == 0 {
                    self.writing.remove(&x);
                }
            }
            if ok {
                self.set(x);
            }
        }
    }

    /// May a fetched copy of `b` be written into the image? (absent and not being written)
    pub fn fillable(&self, b: u64) -> bool {
        !self.is_present(b) && !self.writing.contains_key(&b)
    }

    /// A fetched block was written into the image.
    pub fn filled(&mut self, b: u64) {
        self.set(b);
    }

    /// Evict `b` from the local image (cache budget): it becomes absent unless a write is in
    /// flight. Returns whether it changed.
    pub fn evict(&mut self, b: u64) -> bool {
        let (w, m) = ((b / 64) as usize, 1u64 << (b % 64));
        if self.bits[w] & m == 0 || self.writing.contains_key(&b) {
            return false;
        }
        self.bits[w] &= !m;
        self.absent += 1;
        true
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.bits.iter().flat_map(|w| w.to_le_bytes()).collect()
    }

    /// First absent block at or after `from` (for background hydration).
    pub fn next_absent(&self, from: u64, blocks: u64) -> Option<u64> {
        let mut b = from;
        while b < blocks {
            let w = self.bits[(b / 64) as usize] | ((1u64 << (b % 64)) - 1);
            if w != u64::MAX {
                let x = (b / 64) * 64 + (!w).trailing_zeros() as u64;
                return (x < blocks).then_some(x);
            }
            b = (b / 64 + 1) * 64;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_and_from_bytes() {
        let p = Presence::full(70);
        assert_eq!(p.absent(), 0);
        assert!(p.is_present(69));
        let q = Presence::from_bytes(70, &[]);
        assert_eq!(q.absent(), 70);
        let r = Presence::from_bytes(70, &p.to_bytes());
        assert_eq!(r.absent(), 0);
        let mut s = Presence::from_bytes(10, &[0b101]);
        assert_eq!(s.absent(), 8);
        assert_eq!(s.absent_in(0, 4), vec![1, 3]);
        s.filled(1);
        assert_eq!(s.absent(), 7);
    }

    #[test]
    fn a_fetch_never_overwrites_a_write() {
        let mut p = Presence::from_bytes(8, &[]);
        p.write_begin(2, 2);
        assert!(!p.fillable(2) && !p.fillable(3), "being written: a fetch must not land");
        assert!(p.fillable(4));
        p.write_end(2, 2, true);
        assert!(p.is_present(2) && !p.fillable(2), "written: present, still not fillable");
        p.write_begin(5, 1);
        p.write_end(5, 1, false);
        assert!(p.fillable(5), "a failed write leaves the block absent");
        assert_eq!(p.absent(), 6);
    }

    #[test]
    fn evict_skips_blocks_being_written() {
        let mut p = Presence::full(16);
        p.write_begin(3, 1);
        assert!(!p.evict(3), "a block being written stays");
        assert!(p.evict(4) && !p.evict(4));
        assert_eq!(p.absent(), 1);
        assert!(p.fillable(4));
        p.filled(4);
        assert_eq!(p.absent(), 0);
    }

    #[test]
    fn next_absent_scans_across_words() {
        let mut p = Presence::full(200);
        assert_eq!(p.next_absent(0, 200), None);
        let mut q = Presence::from_bytes(200, &p.to_bytes());
        q.bits[2] &= !(1u64 << 5); // block 133
        q.absent += 1;
        assert_eq!(q.next_absent(0, 200), Some(133));
        assert_eq!(q.next_absent(134, 200), None);
        p = Presence::from_bytes(130, &[]);
        assert_eq!(p.next_absent(129, 130), Some(129));
        assert_eq!(p.next_absent(130, 130), None);
    }
}
