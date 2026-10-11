//! The committed extent map: for every 4 KiB block of a disk, where its committed contents live in
//! the object store (segment, block offset in that segment's data stream), or nothing (a hole).
//! It is updated only when segments commit, in commit order, so a snapshot of it is always
//! consistent with the committed log position. Checkpoints serialize it.
//!
//! The map is a memory-mapped file of u64 entries: `(segment index << 32) | data block`, with
//! segment index 0 meaning "no data". Only touched pages are resident. Segment indexes point into
//! a table of `(epoch, seq)` pairs.
//!
//! Checkpoint object (`<prefix>/ckpt/<epoch>/<seq>`, state after segment `seq` of `epoch`):
//!   "BGC1" | epoch u64 | seq u64 | blocks u64 | zstd( n_segs u32 | n_segs × (epoch u64, seq u64)
//!   | n_runs u64 | n_runs × (block u64, count u32, seg u32, data_block u32) )

use crate::segment::{Entry, KIND_DATA};
use anyhow::{ensure, Result};
use memmap2::MmapMut;
use std::collections::{BTreeMap, HashMap};
use std::fs::OpenOptions;
use std::path::Path;

pub const BLOCK: u64 = 4096;

pub struct ExtentMap {
    map: MmapMut,
    blocks: u64,
    /// segment index → (epoch, seq); index 0 is reserved for "no data"
    pub segs: Vec<(u64, u64)>,
    by_id: HashMap<(u64, u64), u32>,
}

impl ExtentMap {
    /// A zeroed map in `path` (recreated; the map is always rebuilt from the object store on open).
    pub fn create(path: &Path, size: u64) -> Result<Self> {
        let blocks = size.div_ceil(BLOCK);
        let f = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?;
        f.set_len(blocks.max(1) * 8)?;
        let map = unsafe { MmapMut::map_mut(&f)? };
        Ok(ExtentMap { map, blocks, segs: vec![(0, 0)], by_id: HashMap::new() })
    }

    fn entry(&self, b: u64) -> u64 {
        let p = b as usize * 8;
        u64::from_le_bytes(self.map[p..p + 8].try_into().unwrap())
    }
    fn set(&mut self, b: u64, v: u64) {
        let p = b as usize * 8;
        self.map[p..p + 8].copy_from_slice(&v.to_le_bytes());
    }

    /// Where block `b` lives: `(segment index, data block)`, or `None` for a hole.
    pub fn get(&self, b: u64) -> Option<(u32, u32)> {
        let v = self.entry(b);
        (v >> 32 != 0).then_some(((v >> 32) as u32, v as u32))
    }

    pub fn seg_index(&mut self, epoch: u64, seq: u64) -> u32 {
        if let Some(i) = self.by_id.get(&(epoch, seq)) {
            return *i;
        }
        let i = self.segs.len() as u32;
        self.segs.push((epoch, seq));
        self.by_id.insert((epoch, seq), i);
        i
    }

    /// Apply a committed segment's entries (in entry order: a later entry for the same block wins).
    pub fn apply(&mut self, epoch: u64, seq: u64, entries: &[Entry]) {
        let si = self.seg_index(epoch, seq) as u64;
        for e in entries {
            let (b0, n) = (e.off / BLOCK, (e.len as u64).div_ceil(BLOCK));
            for i in 0..n {
                let b = b0 + i;
                if b >= self.blocks {
                    break;
                }
                let v = if e.kind == KIND_DATA { si << 32 | (e.data_off / BLOCK + i) } else { 0 };
                self.set(b, v);
            }
        }
    }

    /// Runs of blocks with consecutive data in the same segment: `(block, count, seg, data_block)`.
    pub fn runs(&self) -> Vec<(u64, u32, u32, u32)> {
        let mut out: Vec<(u64, u32, u32, u32)> = Vec::new();
        for b in 0..self.blocks {
            let Some((s, d)) = self.get(b) else { continue };
            if let Some(last) = out.last_mut() {
                if last.0 + last.1 as u64 == b && last.2 == s && last.3 + last.1 == d && last.1 < u32::MAX {
                    last.1 += 1;
                    continue;
                }
            }
            out.push((b, 1, s, d));
        }
        out
    }

    pub fn encode_checkpoint(&self, epoch: u64, seq: u64) -> Vec<u8> {
        let runs = self.runs();
        let mut body = Vec::with_capacity(4 + self.segs.len() * 16 + 8 + runs.len() * 20);
        body.extend_from_slice(&(self.segs.len() as u32).to_le_bytes());
        for (e, s) in &self.segs {
            body.extend_from_slice(&e.to_le_bytes());
            body.extend_from_slice(&s.to_le_bytes());
        }
        body.extend_from_slice(&(runs.len() as u64).to_le_bytes());
        for (b, n, s, d) in &runs {
            body.extend_from_slice(&b.to_le_bytes());
            body.extend_from_slice(&n.to_le_bytes());
            body.extend_from_slice(&s.to_le_bytes());
            body.extend_from_slice(&d.to_le_bytes());
        }
        let mut out = Vec::new();
        out.extend_from_slice(b"BGC1");
        out.extend_from_slice(&epoch.to_le_bytes());
        out.extend_from_slice(&seq.to_le_bytes());
        out.extend_from_slice(&self.blocks.to_le_bytes());
        out.extend_from_slice(&zstd::bulk::compress(&body, 3).expect("zstd"));
        out
    }

    /// Load a checkpoint into this (fresh) map; returns the log position it reflects.
    pub fn load_checkpoint(&mut self, obj: &[u8]) -> Result<(u64, u64)> {
        ensure!(obj.len() >= 28 && &obj[0..4] == b"BGC1", "not a checkpoint");
        let u64at = |b: &[u8], p: usize| u64::from_le_bytes(b[p..p + 8].try_into().unwrap());
        let u32at = |b: &[u8], p: usize| u32::from_le_bytes(b[p..p + 4].try_into().unwrap());
        let (epoch, seq, blocks) = (u64at(obj, 4), u64at(obj, 12), u64at(obj, 20));
        ensure!(blocks == self.blocks, "checkpoint is for a disk of {blocks} blocks, this disk has {}", self.blocks);
        let body = zstd::stream::decode_all(&obj[28..])?;
        ensure!(body.len() >= 4, "checkpoint body truncated");
        let n_segs = u32at(&body, 0) as usize;
        let runs_at = 4 + n_segs * 16;
        ensure!(body.len() >= runs_at + 8, "checkpoint body truncated");
        let n_runs = u64at(&body, runs_at) as usize;
        ensure!(body.len() == runs_at + 8 + n_runs * 20, "checkpoint body length mismatch");
        self.segs = (0..n_segs).map(|i| (u64at(&body, 4 + i * 16), u64at(&body, 12 + i * 16))).collect();
        ensure!(self.segs.first() == Some(&(0, 0)), "checkpoint segment table must start with the reserved entry");
        self.by_id = self.segs.iter().enumerate().skip(1).map(|(i, k)| (*k, i as u32)).collect();
        for r in 0..n_runs {
            let p = runs_at + 8 + r * 20;
            let (b, n, s, d) = (u64at(&body, p), u32at(&body, p + 8), u32at(&body, p + 12), u32at(&body, p + 16));
            ensure!(s != 0 && (s as usize) < self.segs.len() && b + n as u64 <= self.blocks, "checkpoint run {r} out of range");
            for i in 0..n as u64 {
                self.set(b + i, (s as u64) << 32 | (d as u64 + i));
            }
        }
        Ok((epoch, seq))
    }
}

/// For restoring an image from a map: per segment, the runs to fetch as
/// `(data_block, count, disk_block)`, sorted by data position and merged where contiguous.
pub fn fetch_plan(runs: &[(u64, u32, u32, u32)]) -> BTreeMap<u32, Vec<(u32, u32, u64)>> {
    let mut plan: BTreeMap<u32, Vec<(u32, u32, u64)>> = BTreeMap::new();
    for &(b, n, s, d) in runs {
        plan.entry(s).or_default().push((d, n, b));
    }
    for v in plan.values_mut() {
        v.sort();
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::{Builder, KIND_DISCARD};

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("blockgw-extmap-{name}-{}", std::process::id()))
    }

    #[test]
    fn apply_tracks_latest_location_and_holes() {
        let p = tmp("apply");
        let mut m = ExtentMap::create(&p, 64 * BLOCK).unwrap();
        let mut b = Builder::default();
        b.push_data(0, &vec![1; 4 * BLOCK as usize]); // blocks 0..4 at data blocks 0..4
        b.push_data(10 * BLOCK, &vec![2; BLOCK as usize]); // block 10 at data block 4
        m.apply(1, 0, &b.entries);
        let mut c = Builder::default();
        c.push_data(2 * BLOCK, &vec![3; BLOCK as usize]); // overwrite block 2
        c.push_discard(3 * BLOCK, BLOCK as u32); // discard block 3
        m.apply(1, 1, &c.entries);
        assert_eq!(m.get(0), Some((1, 0)));
        assert_eq!(m.get(1), Some((1, 1)));
        assert_eq!(m.get(2), Some((2, 0)));
        assert_eq!(m.get(3), None);
        assert_eq!(m.get(10), Some((1, 4)));
        assert_eq!(m.get(5), None);
        assert_eq!(m.runs(), vec![(0, 2, 1, 0), (2, 1, 2, 0), (10, 1, 1, 4)]);
        assert_eq!(c.entries[1].kind, KIND_DISCARD);
        std::fs::remove_file(p).unwrap();
    }

    #[test]
    fn checkpoint_round_trip() {
        let (p, q) = (tmp("ck1"), tmp("ck2"));
        let mut m = ExtentMap::create(&p, 1000 * BLOCK).unwrap();
        let mut b = Builder::default();
        b.push_data(5 * BLOCK, &vec![0; 100 * BLOCK as usize]);
        b.push_data(500 * BLOCK, &vec![0; 3 * BLOCK as usize]);
        m.apply(2, 7, &b.entries);
        let mut c = Builder::default();
        c.push_data(50 * BLOCK, &vec![0; BLOCK as usize]);
        m.apply(3, 0, &c.entries);
        let ck = m.encode_checkpoint(3, 0);
        let mut n = ExtentMap::create(&q, 1000 * BLOCK).unwrap();
        assert_eq!(n.load_checkpoint(&ck).unwrap(), (3, 0));
        assert_eq!(n.runs(), m.runs());
        assert_eq!(n.segs, m.segs);
        // new segments keep getting fresh indexes after a load
        assert_eq!(n.seg_index(3, 1), 3);
        let mut wrong = ExtentMap::create(&q, 999 * BLOCK).unwrap();
        assert!(wrong.load_checkpoint(&ck).is_err(), "disk size mismatch");
        let mut bad = ck.clone();
        bad.truncate(ck.len() - 3);
        assert!(ExtentMap::create(&q, 1000 * BLOCK).unwrap().load_checkpoint(&bad).is_err());
        std::fs::remove_file(p).unwrap();
        std::fs::remove_file(q).unwrap();
    }

    #[test]
    fn fetch_plan_groups_by_segment_in_data_order() {
        let runs = vec![(0, 2, 1, 10), (5, 1, 2, 0), (9, 3, 1, 0)];
        let plan = fetch_plan(&runs);
        assert_eq!(plan[&1], vec![(0, 3, 9), (10, 2, 0)]);
        assert_eq!(plan[&2], vec![(0, 1, 5)]);
    }
}
