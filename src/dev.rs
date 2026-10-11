//! One virtual disk: a local sparse image file (the current contents), a write-intent region map,
//! the open segment collecting writes since the last seal, and an ordered upload queue.
//!
//! Durability (docs/design.md §3.2):
//! * every policy: FLUSH => image fdatasync'd; the object store always holds a crash-consistent
//!   prefix of the segment log (segments are sealed in completion order, committed in seq order);
//! * `local`: FLUSH returns after the local fdatasync (with backpressure once the upload lag
//!   exceeds `max_lag`);
//! * `remote`: FLUSH returns only after every segment sealed so far is committed.
//!
//! After a crash of the process or host, regions whose intent bit is set are re-uploaded on the
//! next open before the device is attached.

use crate::extmap::ExtentMap;
use crate::log;
use crate::restore;
use crate::segment::{self, Builder};
use crate::store::{PutOutcome, Store};
use crate::track::{CommitTracker, RegionTracker};
use anyhow::{bail, Result};
use std::collections::{BTreeSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::*};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

pub const REGION: u64 = 4 << 20; // write-intent granularity
pub const MAX_SEG: usize = 16 << 20; // seal early above this
pub const WINDOW: u64 = 8; // concurrent PUTs per disk
pub const FENCE_SPAN: u64 = 2 * WINDOW; // seqs past the old writer's max that a new writer tombstones
pub const CKPT_EVERY: u64 = 64; // checkpoint after this many committed segments...
pub const CKPT_INTERVAL_SECS: u64 = 60; // ...or this long after the last one, if anything committed
pub const POLICY_LOCAL: u8 = 0;
pub const POLICY_REMOTE: u8 = 1;

#[derive(serde::Deserialize, Clone)]
pub struct DevConfig {
    pub id: i32,
    pub size_gb: u64,
    pub dir: String,
    pub prefix: String,
    #[serde(default = "default_policy")]
    pub policy: String,
    #[serde(default = "default_lag")]
    pub max_lag_mb: u64,
    #[serde(default = "default_depth")]
    pub depth: u16,
    #[serde(default = "default_queues")]
    pub queues: u16,
}
fn default_policy() -> String {
    "local".into()
}
fn default_lag() -> u64 {
    16
}
fn default_depth() -> u16 {
    32
}
fn default_queues() -> u16 {
    1
}

struct Sealed {
    seq: u64,
    seg: Arc<Builder>,
}

enum Want {
    Committed(u64),
    LagBelow(u64),
}

#[derive(Default)]
struct Queue {
    sealed: VecDeque<Sealed>, // not yet committed, ascending seq
    bytes: u64,
    dispatched: u64, // seq+1 of the last segment handed to the pool
    commits: CommitTracker,
    waiters: Vec<(Want, RawFd)>,
}

struct Regions {
    t: RegionTracker,
    cleared_unsynced: bool,
    last_sync: Instant,
}

#[derive(Default)]
pub struct Stats {
    pub puts: AtomicU64,
    pub put_bytes: AtomicU64,
    pub raw_bytes: AtomicU64,
    pub flushes: AtomicU64,
    pub intent_syncs: AtomicU64,
    pub lag_waits: AtomicU64,
    pub upload_errors: AtomicU64,
    pub upload_micros: AtomicU64,
    pub flush_wait_micros: AtomicU64,
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct LocalState {
    epoch: u64, // epoch this host last wrote under (0 = never)
}

pub struct Dev {
    pub cfg: DevConfig,
    pub size: u64,
    pub image: File, // O_DIRECT, used by the I/O path
    intent_file: File,
    regions: Mutex<Regions>,
    pending_recovery: Mutex<Vec<u64>>,
    open: Mutex<Builder>,
    queue: Mutex<Queue>,
    /// committed extent map; updated in commit order, together with `committed`
    cmap: Mutex<ExtentMap>,
    store: Arc<dyn Store>,
    last_ckpt: AtomicU64, // `committed` value covered by the last checkpoint
    last_ckpt_at: Mutex<Instant>,
    queue_cv: Condvar,
    pub epoch: u64,
    next_seq: AtomicU64,
    pub committed: AtomicU64, // seq+1 of the highest committed segment (0 = none)
    pub policy: AtomicU8,
    pub max_lag: AtomicU64,
    pub fenced: AtomicBool,
    pub stats: Stats,
}

impl Dev {
    /// Open the disk: claim a new epoch (fencing any previous writer), catch up with segments other
    /// hosts wrote (or restore everything on a fresh host), and note regions a crashed run left dirty.
    pub fn open(cfg: DevConfig, store: Arc<dyn Store>) -> Result<Dev> {
        let s3 = store.as_ref();
        let dir = PathBuf::from(&cfg.dir);
        std::fs::create_dir_all(&dir)?;
        let size = cfg.size_gb << 30;
        let fresh = !dir.join("data").exists();
        let image =
            OpenOptions::new().read(true).write(true).create(true).truncate(false).custom_flags(libc::O_DIRECT).open(dir.join("data"))?;
        image.set_len(size)?;
        let nreg = size.div_ceil(REGION) as usize;
        let intent_file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(dir.join("intent"))?;
        let mut persisted = vec![0u8; nreg.div_ceil(64) * 8];
        let n = intent_file.read_at(&mut persisted, 0).unwrap_or(0);
        let tracker = RegionTracker::new(nreg, &persisted[..n]);
        let dirty = tracker.dirty();
        let st: LocalState = std::fs::read(dir.join("state.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
        let p = cfg.prefix.clone();

        let epoch = claim_epoch(s3, &p)?;

        // Rebuild from the object store: the newest checkpoint, then the segments after it.
        let plan = restore::plan(s3, &p, epoch)?;
        let mut cmap = ExtentMap::create(&dir.join("cmap"), size)?;
        let t0 = Instant::now();
        let foreign = st.epoch == 0 || restore::has_foreign_data(s3, &plan, st.epoch)?;
        if fresh || foreign {
            if !fresh && !dirty.is_empty() && foreign && st.epoch != 0 {
                bail!("disk {} diverged: local writes were never uploaded and another host has written since", cfg.id);
            }
            // the image is rebuilt from the store: start from an empty (all-holes) file
            image.set_len(0)?;
            image.set_len(size)?;
            let replay = OpenOptions::new().write(true).open(dir.join("data"))?;
            let r = restore::restore_full(s3, &p, &plan, &mut cmap, &replay)?;
            replay.sync_all()?;
            crate::info!(
                "disk {}: restored from the object store in {} ms ({} MiB of live data from {}, then {} segments)",
                cfg.id,
                t0.elapsed().as_millis(),
                r.live_bytes >> 20,
                plan.ckpt.as_ref().map(|(k, _)| k.as_str()).unwrap_or("no checkpoint"),
                r.segments
            );
        } else {
            let n = restore::rebuild_map(s3, &p, &plan, &mut cmap)?;
            crate::info!("disk {}: extent map rebuilt in {} ms (checkpoint + {n} segment metas)", cfg.id, t0.elapsed().as_millis());
        }
        let tmp = dir.join("state.json.tmp");
        std::fs::write(&tmp, serde_json::to_vec(&LocalState { epoch })?)?;
        File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, dir.join("state.json"))?;
        crate::info!("disk {}: epoch {epoch} (local was {}), fresh={fresh}, {} dirty regions to re-upload", cfg.id, st.epoch, dirty.len());

        let policy = if cfg.policy == "remote" { POLICY_REMOTE } else { POLICY_LOCAL };
        Ok(Dev {
            max_lag: AtomicU64::new(cfg.max_lag_mb << 20),
            cfg,
            size,
            image,
            intent_file,
            regions: Mutex::new(Regions { t: tracker, cleared_unsynced: false, last_sync: Instant::now() }),
            pending_recovery: Mutex::new(dirty),
            open: Mutex::new(Builder::default()),
            queue: Mutex::new(Queue::default()),
            cmap: Mutex::new(cmap),
            store,
            last_ckpt: AtomicU64::new(0),
            last_ckpt_at: Mutex::new(Instant::now()),
            queue_cv: Condvar::new(),
            epoch,
            next_seq: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            policy: AtomicU8::new(policy),
            fenced: AtomicBool::new(false),
            stats: Stats::default(),
        })
    }

    pub fn snapshot(&self) -> crate::metrics::DiskSnapshot {
        let s = &self.stats;
        crate::metrics::DiskSnapshot {
            id: self.cfg.id,
            epoch: self.epoch,
            policy: if self.policy.load(SeqCst) == POLICY_REMOTE { "remote" } else { "local" },
            fenced: self.fenced.load(SeqCst),
            committed_seq: self.committed.load(SeqCst),
            queued_bytes: self.queued_bytes(),
            open_bytes: self.open_bytes(),
            max_lag_bytes: self.max_lag.load(SeqCst),
            puts: s.puts.load(Relaxed),
            put_bytes: s.put_bytes.load(Relaxed),
            raw_bytes: s.raw_bytes.load(Relaxed),
            upload_errors: s.upload_errors.load(Relaxed),
            upload_seconds: s.upload_micros.load(Relaxed) as f64 / 1e6,
            flushes: s.flushes.load(Relaxed),
            flush_wait_seconds: s.flush_wait_micros.load(Relaxed) as f64 / 1e6,
            intent_syncs: s.intent_syncs.load(Relaxed),
            lag_waits: s.lag_waits.load(Relaxed),
        }
    }

    fn image_path(&self) -> PathBuf {
        PathBuf::from(&self.cfg.dir).join("data")
    }

    /// Mark regions dirty before a write proceeds; only the first write into a clean region pays an
    /// fdatasync. Returns the region range for `intent_end`.
    pub fn intent_begin(&self, off: u64, len: u64) -> Result<(u64, u64)> {
        let (a, b) = (off / REGION, (off + len.max(1) - 1) / REGION);
        let mut g = self.regions.lock().unwrap();
        if g.t.begin(a, b) || g.cleared_unsynced {
            self.persist(&mut g)?;
        }
        Ok((a, b))
    }
    pub fn intent_end(&self, (a, b): (u64, u64), seq: Option<u64>) {
        self.regions.lock().unwrap().t.end(a, b, seq);
    }
    fn persist(&self, g: &mut Regions) -> Result<()> {
        self.intent_file.write_all_at(&g.t.to_bytes(), 0)?;
        self.intent_file.sync_data()?;
        g.cleared_unsynced = false;
        g.last_sync = Instant::now();
        self.stats.intent_syncs.fetch_add(1, Relaxed);
        Ok(())
    }
    /// Persisting a clear is lazy (a stale set bit only costs an extra re-upload after a crash):
    /// at most once a second, or when the queue drained.
    fn clear_committed(&self, drained: bool) {
        let mut g = self.regions.lock().unwrap();
        if g.t.clear_committed(self.committed.load(SeqCst)) {
            g.cleared_unsynced = true;
        }
        if g.cleared_unsynced && (drained || g.last_sync.elapsed().as_secs() >= 1) {
            let _ = self.persist(&mut g);
        }
    }

    /// Add a write to the open segment; returns the seq that segment will be sealed under.
    pub fn append_data(&self, off: u64, d: &[u8]) -> u64 {
        let mut o = self.open.lock().unwrap();
        let seq = self.next_seq.load(SeqCst);
        o.push_data(off, d);
        let full = o.data_len() >= MAX_SEG;
        drop(o);
        if full {
            self.seal();
        }
        seq
    }
    pub fn append_discard(&self, off: u64, len: u32) -> u64 {
        let mut o = self.open.lock().unwrap();
        o.push_discard(off, len);
        self.next_seq.load(SeqCst)
    }

    /// Close the open segment (if non-empty) and queue it. Returns the `committed` value that covers
    /// everything appended so far.
    pub fn seal(&self) -> u64 {
        let mut o = self.open.lock().unwrap();
        if o.is_empty() {
            return self.next_seq.load(SeqCst);
        }
        let seq = self.next_seq.fetch_add(1, SeqCst);
        let seg = std::mem::take(&mut *o);
        let mut q = self.queue.lock().unwrap();
        q.bytes += seg.size() as u64;
        q.sealed.push_back(Sealed { seq, seg: Arc::new(seg) });
        drop(q);
        drop(o);
        self.queue_cv.notify_all();
        seq + 1
    }

    /// Re-upload what a crashed run left behind: the data extents of each dirty region (holes become
    /// discards, found with SEEK_DATA/SEEK_HOLE). Runs before the device is attached.
    ///
    /// Everything goes into a single segment. Uploaded piecewise, a host lost halfway through would
    /// leave a log whose prefix mixes regions at the crash-time state with regions at an older state,
    /// which is not a state the disk was ever in. The segment's size is the dirty data, which write-
    /// intent tracking keeps close to the upload lag at the time of the crash.
    pub fn recover(&self) -> Result<()> {
        let regions = std::mem::take(&mut *self.pending_recovery.lock().unwrap());
        if regions.is_empty() {
            return Ok(());
        }
        let t0 = Instant::now();
        let rd = File::open(self.image_path())?; // buffered: no O_DIRECT alignment rules
        let fd = rd.as_raw_fd();
        let (mut data_bytes, mut holes) = (0u64, 0u64);
        let mut buf = vec![0u8; 1 << 20];
        let mut o = self.open.lock().unwrap();
        let seq = self.next_seq.load(SeqCst); // the open segment is empty: the device is not attached yet
        let mut ranges = Vec::with_capacity(regions.len());
        for r in &regions {
            let (start, end) = (r * REGION, ((r + 1) * REGION).min(self.size));
            ranges.push(self.intent_begin(start, end - start)?);
            let mut pos = start;
            while pos < end {
                let d = unsafe { libc::lseek(fd, pos as i64, libc::SEEK_DATA) };
                let dstart = if d < 0 { end } else { (d as u64).min(end) };
                if dstart > pos {
                    o.push_discard(pos, (dstart - pos) as u32);
                    holes += 1;
                }
                if dstart >= end {
                    break;
                }
                let h = unsafe { libc::lseek(fd, dstart as i64, libc::SEEK_HOLE) };
                let dend = if h < 0 { end } else { (h as u64).min(end) };
                let mut p = dstart;
                while p < dend {
                    let n = ((dend - p) as usize).min(buf.len());
                    rd.read_exact_at(&mut buf[..n], p)?;
                    o.push_data(p, &buf[..n]);
                    data_bytes += n as u64;
                    p += n as u64;
                }
                pos = dend;
            }
        }
        drop(o);
        let need = self.seal();
        for range in ranges {
            self.intent_end(range, Some(seq));
        }
        let mut q = self.queue.lock().unwrap();
        while self.committed.load(SeqCst) < need && !self.fenced.load(SeqCst) {
            q = self.queue_cv.wait(q).unwrap();
        }
        drop(q);
        crate::info!(
            "disk {}: recovery re-uploaded {} regions ({} MiB data, {holes} holes) as one segment in {} ms",
            self.cfg.id,
            regions.len(),
            data_bytes >> 20,
            t0.elapsed().as_millis()
        );
        Ok(())
    }

    pub fn queued_bytes(&self) -> u64 {
        self.queue.lock().unwrap().bytes
    }
    pub fn open_bytes(&self) -> u64 {
        self.open.lock().unwrap().data_len() as u64
    }

    /// Register an eventfd that is written once the condition holds (immediately if it already does).
    pub fn wait_committed(&self, need: u64, efd: RawFd) {
        self.register(Want::Committed(need), efd)
    }
    pub fn wait_lag_below(&self, limit: u64, efd: RawFd) {
        self.register(Want::LagBelow(limit), efd)
    }
    fn register(&self, w: Want, efd: RawFd) {
        let mut q = self.queue.lock().unwrap();
        if self.fenced.load(SeqCst) || self.satisfied(&w, &q) {
            signal(efd);
        } else {
            q.waiters.push((w, efd));
        }
    }
    fn satisfied(&self, w: &Want, q: &Queue) -> bool {
        match w {
            Want::Committed(n) => self.committed.load(SeqCst) >= *n,
            Want::LagBelow(l) => q.bytes <= *l,
        }
    }
    fn wake(&self, q: &mut Queue) {
        let fenced = self.fenced.load(SeqCst);
        for (w, fd) in std::mem::take(&mut q.waiters) {
            if fenced || self.satisfied(&w, q) {
                signal(fd)
            } else {
                q.waiters.push((w, fd))
            }
        }
    }

    /// Upload a checkpoint of the committed extent map if anything committed since the last one.
    /// Returns whether one was written.
    pub fn checkpoint(&self) -> Result<bool> {
        let (bytes, c) = {
            let map = self.cmap.lock().unwrap();
            let c = self.committed.load(SeqCst);
            if c == 0 || c == self.last_ckpt.load(SeqCst) || self.fenced.load(SeqCst) {
                return Ok(false);
            }
            (map.encode_checkpoint(self.epoch, c - 1), c)
        };
        let key = log::key_ckpt(&self.cfg.prefix, self.epoch, c - 1);
        self.store.put_new(&key, &bytes)?; // Exists: an earlier attempt landed; same content
        self.last_ckpt.store(c, SeqCst);
        *self.last_ckpt_at.lock().unwrap() = Instant::now();
        crate::debug!("disk {}: checkpoint {key} ({} bytes)", self.cfg.id, bytes.len());
        Ok(true)
    }

    /// Checkpoint every CKPT_EVERY committed segments, or every CKPT_INTERVAL_SECS if anything committed.
    pub fn checkpointer(&self) {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(2));
            if self.fenced.load(SeqCst) {
                return;
            }
            let (c, last) = (self.committed.load(SeqCst), self.last_ckpt.load(SeqCst));
            let due = c >= last + CKPT_EVERY || (c > last && self.last_ckpt_at.lock().unwrap().elapsed().as_secs() >= CKPT_INTERVAL_SECS);
            if due {
                if let Err(e) = self.checkpoint() {
                    crate::warn!("disk {}: checkpoint failed (will retry): {e:#}", self.cfg.id);
                }
            }
        }
    }

    /// Hands sealed segments to the shared pool in seq order, at most WINDOW beyond the committed prefix.
    pub fn dispatcher(self: &Arc<Self>, pool: &Pool) {
        loop {
            let (seq, raw) = {
                let mut q = self.queue.lock().unwrap();
                loop {
                    if self.fenced.load(SeqCst) {
                        return;
                    }
                    let committed = self.committed.load(SeqCst);
                    let next = q.sealed.iter().find(|s| s.seq >= q.dispatched).map(|s| (s.seq, s.seg.clone()));
                    match next {
                        Some((seq, raw)) if seq < committed + WINDOW => {
                            q.dispatched = seq + 1;
                            break (seq, raw);
                        }
                        _ => q = self.queue_cv.wait(q).unwrap(),
                    }
                }
            };
            pool.jobs.lock().unwrap().push_back((self.clone(), seq, raw));
            pool.cv.notify_one();
        }
    }

    fn complete(&self, seq: u64, raw_len: usize, obj_len: usize, outcome: PutOutcome) {
        let mut q = self.queue.lock().unwrap();
        match outcome {
            PutOutcome::Created => {
                self.stats.puts.fetch_add(1, Relaxed);
                self.stats.put_bytes.fetch_add(obj_len as u64, Relaxed);
                self.stats.raw_bytes.fetch_add(raw_len as u64, Relaxed);
                let newly = q.commits.complete(seq);
                if !newly.is_empty() {
                    // the map and `committed` move together, so a checkpoint is always consistent
                    let mut map = self.cmap.lock().unwrap();
                    for s in newly {
                        let front = q.sealed.pop_front().expect("committed segment is queued");
                        debug_assert_eq!(front.seq, s);
                        q.bytes -= front.seg.size() as u64;
                        map.apply(self.epoch, s, &front.seg.entries);
                    }
                    self.committed.store(q.commits.committed(), SeqCst);
                }
            }
            PutOutcome::Exists => {
                if !self.fenced.swap(true, SeqCst) {
                    crate::error!(
                        "disk {}: FENCED: segment {seq} of epoch {} already exists (a newer writer took over); failing all I/O",
                        self.cfg.id,
                        self.epoch
                    );
                }
            }
        }
        self.wake(&mut q);
        let drained = q.sealed.is_empty();
        drop(q);
        self.queue_cv.notify_all();
        if !self.fenced.load(SeqCst) {
            self.clear_committed(drained);
            if drained {
                unsafe { libc::malloc_trim(0) }; // return freed segment buffers to the OS while idle
            }
        }
    }
}

/// Claim epoch `last+1` with a create-only marker, then tombstone the previous epoch's free seqs so
/// that its writer's next upload collides (docs/design.md §3.4).
fn claim_epoch(s3: &dyn Store, p: &str) -> Result<u64> {
    loop {
        let last = s3.list(&format!("{p}/epoch/"))?.iter().filter_map(|k| log::parse_epoch_key(k)).max().unwrap_or(0);
        if let PutOutcome::Exists = s3.put_new(&log::key_epoch(p, last + 1), b"")? {
            continue; // another writer claimed it first; look again
        }
        if last > 0 {
            let segs: BTreeSet<u64> =
                s3.list(&format!("{p}/seg/{last:010}/"))?.iter().filter_map(|k| log::parse_seg_key(k)).map(|(_, s)| s).collect();
            for sq in log::fence_targets(&segs, FENCE_SPAN) {
                // Exists = the old writer landed this one meanwhile; replay stops at the first tombstone anyway
                s3.put_new(&log::key_seg(p, last, sq), log::TOMB)?;
            }
        }
        return Ok(last + 1);
    }
}

pub fn signal(fd: RawFd) {
    let one: u64 = 1;
    unsafe { libc::write(fd, &one as *const u64 as *const libc::c_void, 8) };
}

type Job = (Arc<Dev>, u64, Arc<Builder>);

/// Process-wide upload pool shared by every disk.
pub struct Pool {
    jobs: Mutex<VecDeque<Job>>,
    cv: Condvar,
}

impl Pool {
    pub fn new() -> Arc<Pool> {
        Arc::new(Pool { jobs: Mutex::new(VecDeque::new()), cv: Condvar::new() })
    }

    /// Compress (skipped for incompressible data), create-only PUT, retry transient errors.
    pub fn worker(&self, s3: &dyn Store) {
        loop {
            let (dev, seq, seg) = {
                let mut j = self.jobs.lock().unwrap();
                while j.is_empty() {
                    j = self.cv.wait(j).unwrap();
                }
                j.pop_front().unwrap()
            };
            if let Err(e) = crate::failpoint::hit("upload") {
                crate::warn!("disk {}: {e}", dev.cfg.id);
            }
            let obj = segment::encode(dev.epoch, seq, &seg);
            let key = log::key_seg(&dev.cfg.prefix, dev.epoch, seq);
            let mut backoff = 100;
            let t0 = Instant::now();
            let outcome = loop {
                if dev.fenced.load(SeqCst) {
                    break PutOutcome::Exists;
                }
                match s3.put_new(&key, &obj) {
                    Ok(o) => {
                        dev.stats.upload_micros.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
                        break o;
                    }
                    Err(e) => {
                        dev.stats.upload_errors.fetch_add(1, Relaxed);
                        crate::warn!("disk {}: upload {key} failed, retrying: {e:#}", dev.cfg.id);
                        std::thread::sleep(std::time::Duration::from_millis(backoff));
                        backoff = (backoff * 2).min(5000);
                    }
                }
            };
            dev.complete(seq, seg.size(), obj.len(), outcome);
        }
    }
}
