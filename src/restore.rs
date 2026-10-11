//! Opening a disk from the object store: pick the newest checkpoint, then the segments after it.
//!
//! * `rebuild_map`: the local image is authoritative (same host, nobody else wrote); only the
//!   extent map is rebuilt, from the checkpoint plus the *meta* of later segments (ranged GETs).
//! * `restore_full`: the image is rebuilt, by fetching the checkpoint's live runs with ranged GETs
//!   and then replaying every later segment in full.
//!
//! Within an epoch only the contiguous prefix `0, 1, 2, ...` is valid, ending at the first
//! tombstone; epochs are replayed in ascending order. Opening a disk seals every older epoch first
//! (`claim_epoch`), so each epoch's run must end at a tombstone: a run that ends at a gap means an
//! object is missing from the store, and the plan fails rather than restore a partial log.

use crate::extmap::{fetch_plan, ExtentMap, BLOCK};
use crate::log;
use crate::segment::{self, KIND_DATA};
use crate::store::Store;
use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;

/// runs to fetch from one segment: (data_block, count, disk_block)
type SegRuns = Vec<(u32, u32, u64)>;

pub struct Plan {
    /// newest checkpoint: key and the log position it reflects
    pub ckpt: Option<(String, (u64, u64))>,
    /// segments after the checkpoint, per epoch in ascending order: (seq, key), up to the tombstone
    pub after: Vec<(u64, Vec<(u64, String)>)>,
}

impl Plan {
    #[cfg(test)]
    pub fn segments(&self) -> usize {
        self.after.iter().map(|(_, v)| v.len()).sum()
    }
}

pub fn plan(store: &dyn Store, prefix: &str, below_epoch: u64) -> Result<Plan> {
    let ckpt = store
        .list(&format!("{prefix}/ckpt/"))?
        .into_iter()
        .filter_map(|k| log::parse_ckpt_key(&k).map(|pos| (k, pos)))
        .filter(|(_, (e, _))| *e < below_epoch)
        .max_by_key(|(_, pos)| *pos);
    let mut by_epoch: BTreeMap<u64, Vec<(u64, String, bool)>> = BTreeMap::new();
    for (k, size) in store.list_sized(&format!("{prefix}/seg/"))? {
        if let Some((e, s)) = log::parse_seg_key(&k) {
            if e < below_epoch {
                by_epoch.entry(e).or_default().push((s, k, size as usize == log::TOMB.len()));
            }
        }
    }
    if let Some((k, (ce, _))) = &ckpt {
        ensure!(by_epoch.contains_key(ce), "log incomplete: {k} exists but its epoch {ce} has no segments or tombstones");
    }
    let mut after = Vec::new();
    for (e, mut keys) in by_epoch {
        keys.sort();
        // only what follows the checkpoint must be contiguous: GC deletes dead segments below it
        let start = match &ckpt {
            Some((_, (ce, _))) if e < *ce => continue, // covered by the checkpoint
            Some((_, (ce, cs))) if e == *ce => cs + 1,
            _ => 0,
        };
        keys.retain(|(s, _, _)| *s >= start);
        let seqs: Vec<u64> = keys.iter().map(|(s, _, _)| *s).collect();
        keys.truncate(log::contiguous_from(&seqs, start));
        match keys.iter().position(|(_, _, tomb)| *tomb) {
            Some(t) => keys.truncate(t),
            None => bail!(
                "log incomplete: epoch {e} breaks off after {} segment(s) from {start} without a tombstone \
                 (a segment is missing from the store{})",
                keys.len(),
                if ckpt.is_none() { ", and no checkpoint covers it" } else { "" }
            ),
        }
        if !keys.is_empty() {
            after.push((e, keys.into_iter().map(|(s, k, _)| (s, k)).collect()));
        }
    }
    Ok(Plan { ckpt, after })
}

/// Fetch header + meta of a segment with ranged GETs; `None` for a tombstone.
pub fn fetch_meta(store: &dyn Store, key: &str) -> Result<Option<segment::Meta>> {
    let mut buf = store.get_range(key, 0, 64 << 10)?;
    if log::is_tomb(&buf) {
        return Ok(None);
    }
    let end = segment::meta_end(&buf).with_context(|| key.to_string())?;
    if buf.len() < end {
        let rest = store.get_range(key, buf.len() as u64, (end - buf.len()) as u64)?;
        buf.extend_from_slice(&rest);
    }
    Ok(Some(segment::parse_meta(&buf).with_context(|| key.to_string())?))
}

fn check_position(meta: &segment::Meta, e: u64, s: u64, key: &str) -> Result<()> {
    ensure!((meta.epoch, meta.seq) == (e, s), "{key}: header says epoch {} seq {}", meta.epoch, meta.seq);
    Ok(())
}

/// Load the checkpoint (if any) into `map`; returns it as a GC reference point.
fn load_checkpoint(store: &dyn Store, plan: &Plan, map: &mut ExtentMap) -> Result<Option<crate::gc::Checkpoint>> {
    let Some((key, pos)) = &plan.ckpt else { return Ok(None) };
    let got = map.load_checkpoint(&store.get(key)?).with_context(|| key.clone())?;
    ensure!(got == *pos, "{key}: checkpoint says position {got:?}");
    Ok(Some(crate::gc::Checkpoint { pos: *pos, referenced: map.referenced() }))
}

/// Rebuild only the extent map (the local image already holds the data). Returns segments applied
/// and the checkpoint it started from.
pub fn rebuild_map(store: &dyn Store, plan: &Plan, map: &mut ExtentMap) -> Result<(usize, Option<crate::gc::Checkpoint>)> {
    let ck = load_checkpoint(store, plan, map)?;
    let mut n = 0;
    for (e, keys) in &plan.after {
        for chunk in keys.chunks(16) {
            let metas: Vec<Result<Option<segment::Meta>>> = std::thread::scope(|sc| {
                let hs: Vec<_> = chunk.iter().map(|(_, k)| sc.spawn(move || fetch_meta(store, k))).collect();
                hs.into_iter().map(|h| h.join().unwrap()).collect()
            });
            let mut ended = false;
            for ((s, k), meta) in chunk.iter().zip(metas) {
                let Some(meta) = meta? else {
                    ended = true;
                    break;
                };
                check_position(&meta, *e, *s, k)?;
                map.apply(*e, *s, &meta.entries);
                n += 1;
            }
            if ended {
                break;
            }
        }
    }
    Ok((n, ck))
}

/// Write the checkpoint's live runs into `image` with ranged GETs (16 segments in parallel).
fn fetch_live_runs(store: &dyn Store, prefix: &str, map: &ExtentMap, image: &File) -> Result<u64> {
    let plan = fetch_plan(&map.runs());
    let jobs: Vec<(u32, SegRuns)> = plan.into_iter().collect();
    let mut bytes = 0u64;
    for chunk in jobs.chunks(16) {
        let results: Vec<Result<u64>> = std::thread::scope(|sc| {
            let hs: Vec<_> = chunk
                .iter()
                .map(|(si, runs)| {
                    let (e, s) = map.segs[*si as usize];
                    sc.spawn(move || fetch_segment_runs(store, &log::key_seg(prefix, e, s), e, s, runs, image))
                })
                .collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for r in results {
            bytes += r?;
        }
    }
    Ok(bytes)
}

/// Frames whose gap is at most this many frames are fetched in one ranged GET.
const MERGE_GAP_FRAMES: usize = 4;

/// Group needed frame indexes (sorted) into spans, bridging gaps of up to `gap` frames; when more
/// than half of all `total` frames are needed, one span covers the whole data.
pub fn frame_spans(needed: &[usize], total: usize, gap: usize) -> Vec<std::ops::Range<usize>> {
    if needed.is_empty() {
        return Vec::new();
    }
    if needed.len() * 2 > total {
        let whole = 0..total; // one span covering every frame
        return vec![whole];
    }
    let mut spans: Vec<std::ops::Range<usize>> = Vec::new();
    for &f in needed {
        match spans.last_mut() {
            Some(s) if f <= s.end + gap => s.end = s.end.max(f + 1),
            _ => spans.push(f..f + 1),
        }
    }
    spans
}

/// Fetch the data runs `(data_block, count, disk_block)` of one segment: the needed frames with as
/// few ranged GETs as reasonable (see `frame_spans`), each frame decoded once.
fn fetch_segment_runs(store: &dyn Store, key: &str, e: u64, s: u64, runs: &[(u32, u32, u64)], image: &File) -> Result<u64> {
    let meta = fetch_meta(store, key)?.with_context(|| format!("{key}: referenced by the checkpoint but is a tombstone"))?;
    check_position(&meta, e, s, key)?;
    let mut needed: Vec<usize> = runs.iter().flat_map(|&(d, n, _)| meta.frames_for(d as u64 * BLOCK, n as u64 * BLOCK)).collect();
    needed.sort_unstable();
    needed.dedup();
    let mut raw: std::collections::HashMap<usize, Vec<u8>> = std::collections::HashMap::new();
    for span in frame_spans(&needed, meta.frames.len(), MERGE_GAP_FRAMES) {
        let (off, len) = segment::frames_span(&meta, &span);
        let bytes = store.get_range(key, off, len)?;
        ensure!(bytes.len() as u64 == len, "{key}: short ranged read");
        for f in span.clone() {
            if needed.binary_search(&f).is_ok() {
                let fr = &meta.frames[f];
                let at = (fr.obj_off - off) as usize;
                raw.insert(f, segment::decode_frame(fr, &bytes[at..at + fr.obj_len as usize])?);
            }
        }
    }
    let fs = meta.frame_size as u64;
    let mut written = 0u64;
    for &(d, n, b) in runs {
        let (mut off, end) = (d as u64 * BLOCK, (d + n) as u64 * BLOCK);
        let mut disk = b * BLOCK;
        while off < end {
            let f = (off / fs) as usize;
            let frame = raw.get(&f).with_context(|| format!("{key}: frame {f} not fetched"))?;
            let at = (off - f as u64 * fs) as usize;
            let take = ((end - off) as usize).min(frame.len() - at);
            image.write_all_at(&frame[at..at + take], disk)?;
            off += take as u64;
            disk += take as u64;
            written += take as u64;
        }
    }
    Ok(written)
}

/// Metas of recently read segments (lazy reads hit the same segments over and over).
#[derive(Default)]
pub struct MetaCache {
    m: std::sync::Mutex<std::collections::HashMap<(u64, u64), std::sync::Arc<segment::Meta>>>,
}

impl MetaCache {
    pub fn get(&self, store: &dyn Store, prefix: &str, e: u64, s: u64) -> Result<std::sync::Arc<segment::Meta>> {
        if let Some(m) = self.m.lock().unwrap().get(&(e, s)) {
            return Ok(m.clone());
        }
        let key = log::key_seg(prefix, e, s);
        let meta = std::sync::Arc::new(fetch_meta(store, &key)?.with_context(|| format!("{key}: referenced but is a tombstone"))?);
        check_position(&meta, e, s, &key)?;
        let mut m = self.m.lock().unwrap();
        if m.len() >= 4096 {
            m.clear();
        }
        m.insert((e, s), meta.clone());
        Ok(meta)
    }
}

/// Fetch the committed contents of individual blocks: `wanted` holds `(disk block, (epoch, seq),
/// data block)`. Returns `(disk block, 4 KiB)` pairs. Frames are fetched per segment with
/// `frame_spans` and decoded once.
pub fn fetch_blocks(store: &dyn Store, prefix: &str, metas: &MetaCache, wanted: &[(u64, (u64, u64), u32)]) -> Result<Vec<(u64, Vec<u8>)>> {
    let mut by_seg: BTreeMap<(u64, u64), Vec<(u32, u64)>> = BTreeMap::new();
    for &(b, seg, d) in wanted {
        by_seg.entry(seg).or_default().push((d, b));
    }
    let mut out = Vec::with_capacity(wanted.len());
    for ((e, s), mut blocks) in by_seg {
        blocks.sort_unstable();
        let key = log::key_seg(prefix, e, s);
        let meta = metas.get(store, prefix, e, s).with_context(|| format!("{key}: segment meta"))?;
        let mut needed: Vec<usize> = blocks.iter().flat_map(|&(d, _)| meta.frames_for(d as u64 * BLOCK, BLOCK)).collect();
        needed.sort_unstable();
        needed.dedup();
        let mut raw: std::collections::HashMap<usize, Vec<u8>> = std::collections::HashMap::new();
        for span in frame_spans(&needed, meta.frames.len(), MERGE_GAP_FRAMES) {
            let (off, len) = segment::frames_span(&meta, &span);
            let bytes = store.get_range(&key, off, len).with_context(|| format!("{key}: ranged read of {len} bytes at {off}"))?;
            ensure!(bytes.len() as u64 == len, "{key}: short ranged read");
            for f in span {
                if needed.binary_search(&f).is_ok() {
                    let fr = &meta.frames[f];
                    let at = (fr.obj_off - off) as usize;
                    raw.insert(f, segment::decode_frame(fr, &bytes[at..at + fr.obj_len as usize])?);
                }
            }
        }
        let fs = meta.frame_size as u64;
        for (d, b) in blocks {
            let (mut off, end) = (d as u64 * BLOCK, (d as u64 + 1) * BLOCK);
            let mut data = Vec::with_capacity(BLOCK as usize);
            while off < end {
                let f = (off / fs) as usize;
                let frame = raw.get(&f).with_context(|| format!("{key}: frame {f} not fetched"))?;
                let at = (off - f as u64 * fs) as usize;
                let take = ((end - off) as usize).min(frame.len() - at);
                data.extend_from_slice(&frame[at..at + take]);
                off += take as u64;
            }
            out.push((b, data));
        }
    }
    Ok(out)
}

/// Apply a whole segment object to the image and the map.
fn apply_object(obj: &[u8], e: u64, s: u64, key: &str, image: &File, map: &mut ExtentMap) -> Result<()> {
    let (meta, data) = segment::decode_all(obj).with_context(|| key.to_string())?;
    check_position(&meta, e, s, key)?;
    for en in &meta.entries {
        if en.kind == KIND_DATA {
            image.write_all_at(&data[en.data_off as usize..en.data_off as usize + en.len as usize], en.off)?;
        } else {
            log::punch(image.as_raw_fd(), en.off, en.len as u64)?;
        }
    }
    map.apply(e, s, &meta.entries);
    Ok(())
}

pub struct Restored {
    pub live_bytes: u64,
    pub segments: usize,
    pub ckpt: Option<crate::gc::Checkpoint>,
}

/// Rebuild `image` (expected empty) and `map` from the object store.
pub fn restore_full(store: &dyn Store, prefix: &str, plan: &Plan, map: &mut ExtentMap, image: &File) -> Result<Restored> {
    let ckpt = load_checkpoint(store, plan, map)?;
    let live_bytes = fetch_live_runs(store, prefix, map, image)?;
    let mut segments = 0;
    for (e, keys) in &plan.after {
        'epoch: for chunk in keys.chunks(16) {
            let objs: Vec<Result<Vec<u8>>> = std::thread::scope(|sc| {
                let hs: Vec<_> = chunk.iter().map(|(_, k)| sc.spawn(move || store.get(k))).collect();
                hs.into_iter().map(|h| h.join().unwrap()).collect()
            });
            for ((s, k), obj) in chunk.iter().zip(objs) {
                let obj = obj?;
                if log::is_tomb(&obj) {
                    break 'epoch;
                }
                apply_object(&obj, *e, *s, k, image, map)?;
                segments += 1;
            }
        }
    }
    Ok(Restored { live_bytes, segments, ckpt })
}

/// True if some epoch in `(after_epoch, ..)` of the plan, or the checkpoint, carries data written
/// by another host (a tombstone at the start of an epoch means its owner never wrote).
pub fn has_foreign_data(store: &dyn Store, plan: &Plan, after_epoch: u64) -> Result<bool> {
    if let Some((_, (ce, _))) = &plan.ckpt {
        if *ce > after_epoch {
            return Ok(true);
        }
    }
    for (e, keys) in &plan.after {
        if *e > after_epoch {
            if let Some((_, k)) = keys.first() {
                if !log::is_tomb(&store.get_range(k, 0, 4)?) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::Builder;
    use crate::store::LocalDir;

    fn root(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("blockgw-restore-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn put_seg(st: &LocalDir, e: u64, s: u64, b: &Builder) {
        st.put_new(&log::key_seg("d", e, s), &segment::encode(e, s, 0, b)).unwrap();
    }

    /// what a later claim does to an epoch: a tombstone ends it
    fn tomb(st: &LocalDir, e: u64, s: u64) {
        st.put_new(&log::key_seg("d", e, s), log::TOMB).unwrap();
    }

    fn block(v: u8) -> Vec<u8> {
        vec![v; BLOCK as usize]
    }

    /// Build a log, checkpoint part of it, and check that a full restore and a map rebuild agree with
    /// a straight replay of every segment.
    #[test]
    fn checkpoint_then_replay_equals_full_replay() {
        let r = root("full");
        let st = LocalDir::new(r.join("store")).unwrap();
        let size = 64 * BLOCK;
        let mut truth = ExtentMap::create(&r.join("truth"), size).unwrap();
        let mut expect = vec![0u8; size as usize];
        let mut writes = |st: &LocalDir, truth: &mut ExtentMap, e: u64, s: u64, items: &[(u64, u8)], discard: Option<u64>| {
            let mut b = Builder::default();
            for &(blk, v) in items {
                b.push_data(blk * BLOCK, &block(v));
                expect[(blk * BLOCK) as usize..((blk + 1) * BLOCK) as usize].copy_from_slice(&block(v));
            }
            if let Some(blk) = discard {
                b.push_discard(blk * BLOCK, BLOCK as u32);
                expect[(blk * BLOCK) as usize..((blk + 1) * BLOCK) as usize].fill(0);
            }
            put_seg(st, e, s, &b);
            truth.apply(e, s, &b.entries);
        };
        writes(&st, &mut truth, 1, 0, &[(0, 1), (1, 1), (2, 1)], None);
        writes(&st, &mut truth, 1, 1, &[(1, 2), (9, 2)], None);
        st.put_new(&log::key_ckpt("d", 1, 1), &truth.encode_checkpoint(1, 1)).unwrap();
        writes(&st, &mut truth, 1, 2, &[(2, 3)], Some(9));
        st.put_new(&log::key_seg("d", 1, 3), log::TOMB).unwrap(); // a later writer fenced epoch 1 here
        st.put_new(&log::key_seg("d", 1, 4), b"junk past the tombstone").unwrap();
        writes(&st, &mut truth, 2, 0, &[(0, 4), (30, 4)], None);
        tomb(&st, 2, 1); // the claim of epoch 3 sealed epoch 2

        let plan = plan(&st, "d", 3).unwrap();
        assert_eq!(plan.ckpt.as_ref().unwrap().1, (1, 1));
        assert_eq!(plan.segments(), 2, "epoch 1 seq 2 (up to its tombstone) and epoch 2 seq 0");

        let img = std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(true).open(r.join("img")).unwrap();
        img.set_len(size).unwrap();
        let mut map = ExtentMap::create(&r.join("map"), size).unwrap();
        let got = restore_full(&st, "d", &plan, &mut map, &img).unwrap();
        assert_eq!(got.segments, 2, "stops at the tombstone in epoch 1, then epoch 2");
        let mut buf = vec![0u8; size as usize];
        img.read_exact_at(&mut buf, 0).unwrap();
        assert!(buf == expect, "restored image differs from a full replay");
        assert_eq!(map.runs(), truth.runs());

        let mut map2 = ExtentMap::create(&r.join("map2"), size).unwrap();
        let (n, ck) = rebuild_map(&st, &plan, &mut map2).unwrap();
        assert_eq!(n, 2);
        assert_eq!(ck.unwrap().pos, (1, 1));
        assert_eq!(got.ckpt.unwrap().pos, (1, 1));
        assert_eq!(map2.runs(), truth.runs());
        assert!(has_foreign_data(&st, &plan, 1).unwrap(), "epoch 2 wrote data");
        assert!(!has_foreign_data(&st, &plan, 2).unwrap());
        std::fs::remove_dir_all(r).unwrap();
    }

    #[test]
    fn plan_without_checkpoint_takes_every_sealed_prefix() {
        let r = root("nockpt");
        let st = LocalDir::new(&r).unwrap();
        let b = Builder::default();
        for (e, s) in [(1, 0), (1, 1), (1, 3), (2, 0), (5, 0)] {
            put_seg(&st, e, s, &b);
        }
        // the fencing claims: epoch 1's writer had 1/3 in flight when 1/2 was fenced
        tomb(&st, 1, 2);
        tomb(&st, 2, 1);
        tomb(&st, 3, 0); // a writer that never uploaded
        let p = plan(&st, "d", 5).unwrap();
        assert!(p.ckpt.is_none());
        let shape: Vec<(u64, Vec<u64>)> = p.after.iter().map(|(e, v)| (*e, v.iter().map(|(s, _)| *s).collect())).collect();
        assert_eq!(shape, vec![(1, vec![0, 1]), (2, vec![0])], "tombstone at 1/2 ends epoch 1; epoch 5 is excluded");
        std::fs::remove_dir_all(r).unwrap();
    }

    #[test]
    fn a_missing_segment_fails_the_plan_instead_of_restoring_a_partial_log() {
        let r = root("missing");
        let st = LocalDir::new(&r).unwrap();
        let b = Builder::default();
        // epoch 1: 0..=3 then a tombstone, but 2 was lost
        for s in [0, 1, 3] {
            put_seg(&st, 1, s, &b);
        }
        tomb(&st, 1, 4);
        put_seg(&st, 2, 0, &b);
        tomb(&st, 2, 1);
        let e = plan(&st, "d", 3).err().expect("a hole in the log must fail the plan").to_string();
        assert!(e.contains("log incomplete") && e.contains("epoch 1"), "{e}");
        // the start of epoch 1 lost (as a copy taken across a GC round without its checkpoint)
        let r2 = root("missing-start");
        let st2 = LocalDir::new(&r2).unwrap();
        for s in [5, 6] {
            put_seg(&st2, 1, s, &b);
        }
        tomb(&st2, 1, 7);
        put_seg(&st2, 2, 0, &b);
        tomb(&st2, 2, 1);
        let e = plan(&st2, "d", 3).err().expect("no checkpoint covers 1/0..=4").to_string();
        assert!(e.contains("no checkpoint covers it"), "{e}");
        // a checkpoint whose epoch vanished entirely
        st2.put_new(&log::key_ckpt("d", 4, 3), b"x").unwrap();
        assert!(plan(&st2, "d", 9).is_err());
        std::fs::remove_dir_all(r).unwrap();
        std::fs::remove_dir_all(r2).unwrap();
    }

    #[test]
    fn segments_deleted_below_the_checkpoint_do_not_end_the_epoch() {
        let r = root("gcgap");
        let st = LocalDir::new(&r).unwrap();
        let b = Builder::default();
        for s in [3, 5, 6, 7, 9] {
            put_seg(&st, 1, s, &b); // 0..=2 and 4 were collected; 8 was in flight when fenced
        }
        tomb(&st, 1, 8);
        tomb(&st, 1, 10);
        st.put_new(&log::key_ckpt("d", 1, 4), b"x").unwrap();
        let p = plan(&st, "d", 2).unwrap();
        let seqs: Vec<u64> = p.after[0].1.iter().map(|(s, _)| *s).collect();
        assert_eq!(seqs, vec![5, 6, 7]);
        std::fs::remove_dir_all(r).unwrap();
    }

    #[test]
    fn frame_spans_bridge_small_gaps_and_go_whole_when_dense() {
        assert!(frame_spans(&[], 10, 4).is_empty());
        assert_eq!(frame_spans(&[0, 2, 9], 40, 4), vec![0..3, 9..10]);
        assert_eq!(frame_spans(&[0, 5, 30], 40, 4), vec![0..6, 30..31]);
        assert_eq!(frame_spans(&[1, 3, 5, 7, 9, 11], 10, 0), vec![0..10], "more than half needed: one GET");
    }
}
