//! The segment log: object keys, the segment and object formats, and the pure rules that decide
//! what is valid to replay and which keys a new writer must occupy to fence an old one.
//!
//! Object layout under a disk prefix `P`:
//!   `P/epoch/<e:010>`          empty marker, created (create-only) by the writer that owns epoch `e`
//!   `P/seg/<e:010>/<s:012>`    segment `s` of epoch `e`, or the 4-byte tombstone `TOMB`
//!
//! Segment (before the object codec byte):
//!   "BGS1" | epoch u64 | seq u64 | n u32 | n × (kind u8, offset u64, len u32) | data of the kind-0 entries
//! Object: codec u8 (0 = raw, 1 = zstd) followed by the segment bytes.

use anyhow::{bail, Result};
use std::collections::BTreeSet;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::os::unix::io::{AsRawFd, RawFd};

pub const MAGIC: &[u8; 4] = b"BGS1";
pub const TOMB: &[u8] = b"TOMB";
const HEADER: usize = 24;
const ENTRY: usize = 13;
pub const KIND_DATA: u8 = 0;
pub const KIND_DISCARD: u8 = 1;

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

/// Writes collected since the last seal.
#[derive(Default)]
pub struct SegmentBuilder {
    entries: Vec<(u8, u64, u32)>,
    data: Vec<u8>,
}

impl SegmentBuilder {
    pub fn push_data(&mut self, off: u64, d: &[u8]) {
        self.entries.push((KIND_DATA, off, d.len() as u32));
        self.data.extend_from_slice(d);
    }
    pub fn push_discard(&mut self, off: u64, len: u32) {
        self.entries.push((KIND_DISCARD, off, len));
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn data_len(&self) -> usize {
        self.data.len()
    }
    pub fn encode(&self, epoch: u64, seq: u64) -> Vec<u8> {
        let mut v = Vec::with_capacity(HEADER + self.entries.len() * ENTRY + self.data.len());
        v.extend_from_slice(MAGIC);
        v.extend_from_slice(&epoch.to_le_bytes());
        v.extend_from_slice(&seq.to_le_bytes());
        v.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (k, off, len) in &self.entries {
            v.push(*k);
            v.extend_from_slice(&off.to_le_bytes());
            v.extend_from_slice(&len.to_le_bytes());
        }
        v.extend_from_slice(&self.data);
        v
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Op<'a> {
    Write { off: u64, data: &'a [u8] },
    Discard { off: u64, len: u32 },
}

#[derive(Debug)]
pub struct Segment<'a> {
    pub epoch: u64,
    pub seq: u64,
    pub ops: Vec<Op<'a>>,
}

/// Parse a segment, validating every length against the buffer.
pub fn decode(raw: &[u8]) -> Result<Segment<'_>> {
    if raw.len() < HEADER || &raw[0..4] != MAGIC {
        bail!("not a segment (bad magic or short header)");
    }
    let epoch = u64::from_le_bytes(raw[4..12].try_into()?);
    let seq = u64::from_le_bytes(raw[12..20].try_into()?);
    let n = u32::from_le_bytes(raw[20..24].try_into()?) as usize;
    let table_end = n.checked_mul(ENTRY).and_then(|t| t.checked_add(HEADER)).filter(|e| *e <= raw.len());
    let Some(mut d) = table_end else { bail!("segment entry table exceeds the buffer") };
    let mut ops = Vec::with_capacity(n);
    for i in 0..n {
        let p = HEADER + i * ENTRY;
        let kind = raw[p];
        let off = u64::from_le_bytes(raw[p + 1..p + 9].try_into()?);
        let len = u32::from_le_bytes(raw[p + 9..p + 13].try_into()?);
        match kind {
            KIND_DATA => {
                let end = d + len as usize;
                if end > raw.len() {
                    bail!("segment data entry {i} exceeds the buffer");
                }
                ops.push(Op::Write { off, data: &raw[d..end] });
                d = end;
            }
            KIND_DISCARD => ops.push(Op::Discard { off, len }),
            k => bail!("unknown entry kind {k}"),
        }
    }
    if d != raw.len() {
        bail!("segment has {} trailing bytes", raw.len() - d);
    }
    Ok(Segment { epoch, seq, ops })
}

/// Apply a segment to an image file (buffered handle: payloads are not O_DIRECT-aligned), after
/// checking that its header matches the key it was stored under.
pub fn apply(image: &File, raw: &[u8], epoch: u64, seq: u64) -> Result<()> {
    let seg = decode(raw)?;
    if (seg.epoch, seg.seq) != (epoch, seq) {
        bail!("segment header says epoch {} seq {}, key says epoch {epoch} seq {seq}", seg.epoch, seg.seq);
    }
    for op in seg.ops {
        match op {
            Op::Write { off, data } => image.write_all_at(data, off)?,
            Op::Discard { off, len } => punch(image.as_raw_fd(), off, len as u64)?,
        }
    }
    Ok(())
}

pub fn punch(fd: RawFd, off: u64, len: u64) -> Result<()> {
    let r = unsafe { libc::fallocate(fd, libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE, off as i64, len as i64) };
    if r != 0 {
        bail!("fallocate(PUNCH_HOLE): {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Wrap a segment into an object: zstd when a 256 KiB sample compresses by >10% and the whole
/// segment by >5%, raw otherwise (media, model weights, encrypted data).
pub fn to_object(raw: &[u8]) -> Vec<u8> {
    let sample = &raw[..raw.len().min(256 << 10)];
    let worth = zstd::encode_all(sample, 1).map(|z| z.len() < sample.len() * 9 / 10).unwrap_or(false);
    match worth.then(|| zstd::encode_all(raw, 1).ok()).flatten() {
        Some(z) if z.len() < raw.len() * 95 / 100 => std::iter::once(1u8).chain(z).collect(),
        _ => std::iter::once(0u8).chain(raw.iter().copied()).collect(),
    }
}

/// Segment bytes of an object, or `None` for a tombstone.
pub fn from_object(obj: &[u8]) -> Result<Option<Vec<u8>>> {
    if obj == TOMB {
        return Ok(None);
    }
    match obj.first() {
        Some(0) => Ok(Some(obj[1..].to_vec())),
        Some(1) => Ok(Some(zstd::decode_all(&obj[1..])?)),
        _ => bail!("unknown object codec"),
    }
}

/// How many segments of one epoch form the replayable prefix, judged by key presence only:
/// `seqs` must be sorted; the prefix is 0, 1, 2, ... up to the first gap. (Replay additionally
/// stops at the first tombstone, which is only known after fetching.)
pub fn contiguous_prefix(seqs: &[u64]) -> usize {
    seqs.iter().enumerate().take_while(|(i, s)| **s == *i as u64).count()
}

/// Keys a new writer must occupy (with tombstones) in the previous epoch so that every PUT the old
/// writer may still have in flight collides: every free seq from the first gap up to max + span.
/// An old writer keeps at most `window` PUTs in flight beyond its contiguous prefix, so
/// `span >= window` covers them.
pub fn fence_targets(existing: &BTreeSet<u64>, span: u64) -> Vec<u64> {
    let first_gap = (0..).find(|i| !existing.contains(i)).unwrap();
    let max = existing.iter().next_back().copied().unwrap_or(0);
    (first_gap..=max + span).filter(|s| !existing.contains(s)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg() -> Vec<u8> {
        let mut b = SegmentBuilder::default();
        b.push_data(4096, &[1u8; 4096]);
        b.push_discard(1 << 20, 65536);
        b.push_data(0, b"hello");
        b.encode(3, 7)
    }

    #[test]
    fn keys_round_trip() {
        assert_eq!(parse_seg_key(&key_seg("a/b", 3, 42)), Some((3, 42)));
        assert_eq!(parse_epoch_key(&key_epoch("a/b", 9)), Some(9));
        assert_eq!(parse_seg_key(&key_epoch("a", 9)), None);
        assert_eq!(parse_seg_key("a/seg/x/1"), None);
        // zero-padded keys sort numerically
        assert!(key_seg("p", 1, 9) < key_seg("p", 1, 10));
        assert!(key_seg("p", 9, 999) < key_seg("p", 10, 0));
    }

    #[test]
    fn segment_round_trip() {
        let raw = seg();
        let s = decode(&raw).unwrap();
        assert_eq!((s.epoch, s.seq), (3, 7));
        assert_eq!(
            s.ops,
            vec![
                Op::Write { off: 4096, data: &[1u8; 4096] },
                Op::Discard { off: 1 << 20, len: 65536 },
                Op::Write { off: 0, data: b"hello" },
            ]
        );
        assert!(SegmentBuilder::default().is_empty());
    }

    #[test]
    fn decode_rejects_corruption() {
        let raw = seg();
        assert!(decode(&raw[..10]).is_err(), "short header");
        assert!(decode(&raw[..raw.len() - 1]).is_err(), "truncated data");
        let mut extra = raw.clone();
        extra.push(0);
        assert!(decode(&extra).is_err(), "trailing bytes");
        let mut bad = raw.clone();
        bad[0] = b'X';
        assert!(decode(&bad).is_err(), "magic");
        let mut huge = raw.clone();
        huge[20..24].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&huge).is_err(), "entry count overflow");
        let mut kind = raw;
        kind[HEADER] = 9;
        assert!(decode(&kind).is_err(), "unknown kind");
    }

    #[test]
    fn object_codec() {
        let compressible = SegmentBuilder { entries: vec![(0, 0, 1 << 20)], data: vec![7u8; 1 << 20] }.encode(1, 0);
        let o = to_object(&compressible);
        assert_eq!(o[0], 1);
        assert!(o.len() < compressible.len() / 10);
        assert_eq!(from_object(&o).unwrap().unwrap(), compressible);

        let mut noise = vec![0u8; 1 << 20];
        let mut x = 0x9e3779b97f4a7c15u64;
        for b in noise.iter_mut() {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        let incompressible = SegmentBuilder { entries: vec![(0, 0, noise.len() as u32)], data: noise }.encode(1, 1);
        let o = to_object(&incompressible);
        assert_eq!(o[0], 0, "incompressible data is stored raw");
        assert_eq!(from_object(&o).unwrap().unwrap(), incompressible);

        assert_eq!(from_object(TOMB).unwrap(), None);
        assert!(from_object(&[5, 1, 2]).is_err());
    }

    #[test]
    fn apply_writes_and_punches() {
        let path = std::env::temp_dir().join(format!("blockgw-apply-{}", std::process::id()));
        let f = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
        f.set_len(2 << 20).unwrap();
        f.write_all_at(&[9u8; 65536], 1 << 20).unwrap();
        assert!(apply(&f, &seg(), 3, 8).is_err(), "header/key mismatch is rejected");
        apply(&f, &seg(), 3, 7).unwrap();
        let mut b = vec![0u8; 5];
        f.read_exact_at(&mut b, 0).unwrap();
        assert_eq!(&b, b"hello");
        let mut b = vec![0u8; 4096];
        f.read_exact_at(&mut b, 4096).unwrap();
        assert!(b.iter().all(|x| *x == 1));
        let mut b = vec![0u8; 65536];
        f.read_exact_at(&mut b, 1 << 20).unwrap();
        assert!(b.iter().all(|x| *x == 0), "discarded range reads as zeros");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn prefix_stops_at_first_gap() {
        assert_eq!(contiguous_prefix(&[]), 0);
        assert_eq!(contiguous_prefix(&[0, 1, 2]), 3);
        assert_eq!(contiguous_prefix(&[0, 1, 3, 4]), 2);
        assert_eq!(contiguous_prefix(&[1, 2]), 0);
    }

    #[test]
    fn fence_covers_gaps_and_in_flight_window() {
        let set = |v: &[u64]| v.iter().copied().collect::<BTreeSet<u64>>();
        // nothing written yet: occupy 0..=span
        assert_eq!(fence_targets(&set(&[]), 3), vec![0, 1, 2, 3]);
        // contiguous 0..=4: occupy 5..=4+span
        assert_eq!(fence_targets(&set(&[0, 1, 2, 3, 4]), 2), vec![5, 6]);
        // out-of-order uploads left gaps at 2 and 4: fill them and the window past the max
        assert_eq!(fence_targets(&set(&[0, 1, 3, 5]), 2), vec![2, 4, 6, 7]);
    }
}
