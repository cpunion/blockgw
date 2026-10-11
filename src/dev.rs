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
use anyhow::{bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering::*};
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
    /// checkpoint after this many committed segments (or CKPT_INTERVAL_SECS)
    #[serde(default = "default_ckpt_every")]
    pub checkpoint_every: u64,
    /// garbage-collect dead segments and repack sparse ones after each checkpoint
    #[serde(default = "default_true")]
    pub gc: bool,
    /// repack segments whose live fraction is below this
    #[serde(default = "default_repack_ratio")]
    pub repack_ratio: f64,
    /// live data rewritten per GC round
    #[serde(default = "default_repack_budget")]
    pub repack_budget_mb: u64,
    /// restore lazily: attach at once and fetch blocks from the object store when first read
    #[serde(default = "default_true")]
    pub lazy: bool,
    /// background hydration rate after a lazy restore (0 = only on demand)
    #[serde(default = "default_hydrate")]
    pub hydrate_mbps: u64,
    /// encrypt every object of this disk with the 32-byte key in this file (see crypt.rs)
    #[serde(default)]
    pub key_file: Option<String>,
    /// earlier keys of this disk, used only to read objects sealed before a rotation (`ctl rekey`)
    #[serde(default)]
    pub old_key_files: Vec<String>,
    /// also checkpoint after this much data was uploaded since the last checkpoint
    #[serde(default = "default_ckpt_mb")]
    pub checkpoint_mb: u64,
    /// seal the open segment once its oldest write is this old (0 = only on flush and size). Under
    /// `local` a flush does not seal, so this batches the writes of many flushes into one PUT.
    #[serde(default = "default_seal_interval")]
    pub seal_interval_ms: u64,
    /// keep at most this much of the disk's data in the local image (0 = all of it): cold regions
    /// whose writes are all committed are dropped and fetched again when read
    #[serde(default)]
    pub cache_mb: u64,
    /// "host:port" of a standby (`blockgw replica`) that keeps acknowledged writes until they are
    /// in the object store; under `local`, a flush then returns once the standby has them
    #[serde(default)]
    pub replica: Option<String>,
}
fn default_ckpt_mb() -> u64 {
    64
}
fn default_seal_interval() -> u64 {
    1000
}

/// Milliseconds on a monotonic clock (0 is never returned).
fn mono_ms() -> u64 {
    static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    T0.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1
}
fn default_hydrate() -> u64 {
    64
}
fn default_ckpt_every() -> u64 {
    CKPT_EVERY
}
fn default_true() -> bool {
    true
}
fn default_repack_ratio() -> f64 {
    0.5
}
fn default_repack_budget() -> u64 {
    64
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
    pub gc_deleted_segments: AtomicU64,
    pub gc_deleted_bytes: AtomicU64,
    pub gc_repacked_bytes: AtomicU64,
    pub evicted_bytes: AtomicU64,
    pub fetched_bytes: AtomicU64,
    pub fetch_errors: AtomicU64,
}

/// Lazy-read state: which blocks the image holds, and which bitmap pages changed since the last
/// flush persisted them.
struct LazyState {
    p: crate::presence::Presence,
    dirty_pages: BTreeSet<usize>,
    file_present: bool,                           // a presence file exists on disk
    reading: std::collections::HashMap<u64, u32>, // region -> reads in flight (cache eviction only)
}

const PRESENCE_PAGE: usize = 4096; // bytes of bitmap per persisted page (32768 blocks)

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct LocalState {
    epoch: u64, // epoch this host last wrote under (0 = never)
}

pub struct Dev {
    pub cfg: DevConfig,
    pub size: u64,
    pub image: File, // O_DIRECT, used by the I/O path
    image_buf: File,
    /// buffered handle to the same image, opened once: fetch fills, recovery and repack use it, so a
    /// later mount over (or rename of) the image's directory cannot break them
    intent_file: File,
    regions: Mutex<Regions>,
    pending_recovery: Mutex<Vec<u64>>,
    open: Mutex<Builder>,
    open_since: AtomicU64, // mono_ms() of the open segment's first append, 0 when empty
    queue: Mutex<Queue>,
    /// committed extent map; updated in commit order, together with `committed`
    cmap: Mutex<ExtentMap>,
    store: Arc<dyn Store>,
    last_ckpt: AtomicU64,      // `committed` value covered by the last checkpoint
    last_ckpt_user: AtomicU64, // user_bytes when the last checkpoint was written
    user_bytes: AtomicU64,     // data appended by writes (not by repack), for checkpoint_mb
    last_ckpt_at: Mutex<Instant>,
    /// the last two checkpoints written by this server (GC deletes against both)
    ckpts: Mutex<VecDeque<crate::gc::Checkpoint>>,
    lazy: Mutex<LazyState>,
    fully_present: AtomicBool,
    presence_file: File,
    /// per region, mono_ms()/1000 of the last read or write (cache eviction)
    access: Vec<AtomicU32>,
    /// rekey needs every block local: no eviction meanwhile
    evict_paused: AtomicBool,
    pub rep: Option<Arc<crate::replica::Replicator>>,
    /// what of the open segment the standby has: (seq, entries sent, next batch number); locked
    /// only while `open` is held
    rep_cursor: Mutex<(u64, usize, u32)>,
    metas: restore::MetaCache,
    queue_cv: Condvar,
    pub epoch: u64,
    next_seq: AtomicU64,
    pub committed: AtomicU64, // seq+1 of the highest committed segment (0 = none)
    pub policy: AtomicU8,
    pub max_lag: AtomicU64,
    pub fenced: AtomicBool,
    /// set when the disk is detached: its background threads exit
    stopped: AtomicBool,
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

        probe_readable(s3, &p).with_context(|| format!("disk {}: cannot read its log (nothing was changed)", cfg.id))?;
        if let Some(addr) = &cfg.replica {
            take_over_journal(s3, &p, addr, st.epoch).with_context(|| format!("disk {}", cfg.id))?;
        }
        let epoch = claim_epoch(s3, &p)?;

        // Rebuild from the object store: the newest checkpoint, then the segments after it.
        let plan = restore::plan(s3, &p, epoch)?;
        let mut cmap = ExtentMap::create(&dir.join("cmap"), size)?;
        let t0 = Instant::now();
        let foreign = st.epoch == 0 || restore::has_foreign_data(s3, &plan, st.epoch)?;
        let loaded_ckpt;
        let blocks = size.div_ceil(crate::extmap::BLOCK);
        let presence_path = dir.join("presence");
        let presence;
        if fresh || foreign {
            if !fresh && !dirty.is_empty() && foreign && st.epoch != 0 {
                bail!("disk {} diverged: local writes were never uploaded and another host has written since", cfg.id);
            }
            // the image is rebuilt from the store: start from an empty (all-holes) file
            image.set_len(0)?;
            image.set_len(size)?;
            if cfg.lazy {
                let (n, ck) = restore::rebuild_map(s3, &plan, &mut cmap)?;
                loaded_ckpt = ck;
                let mut p = crate::presence::Presence::full(blocks);
                let mut absent = crate::presence::Presence::from_bytes(blocks, &[]);
                for (b, c, _, _) in cmap.runs() {
                    for x in b..b + c as u64 {
                        absent.filled(x);
                    }
                }
                // present = every block the map does not point at (holes read as zeros)
                p = crate::presence::Presence::from_bytes(blocks, &invert(&absent.to_bytes(), &p.to_bytes()));
                write_synced(&presence_path, &p.to_bytes())?;
                crate::info!(
                    "disk {}: lazy restore in {} ms (checkpoint + {n} segment metas); {} blocks to fetch on demand",
                    cfg.id,
                    t0.elapsed().as_millis(),
                    p.absent()
                );
                presence = p;
            } else {
                let replay = OpenOptions::new().write(true).open(dir.join("data"))?;
                let mut r = restore::restore_full(s3, &p, &plan, &mut cmap, &replay)?;
                loaded_ckpt = r.ckpt.take();
                replay.sync_all()?;
                let _ = std::fs::remove_file(&presence_path);
                crate::info!(
                    "disk {}: restored from the object store in {} ms ({} MiB of live data from {}, then {} segments)",
                    cfg.id,
                    t0.elapsed().as_millis(),
                    r.live_bytes >> 20,
                    plan.ckpt.as_ref().map(|(k, _)| k.as_str()).unwrap_or("no checkpoint"),
                    r.segments
                );
                presence = crate::presence::Presence::full(blocks);
            }
        } else {
            let (n, ck) = restore::rebuild_map(s3, &plan, &mut cmap)?;
            loaded_ckpt = ck;
            presence = match std::fs::read(&presence_path) {
                Ok(b) => crate::presence::Presence::from_bytes(blocks, &b),
                Err(_) => crate::presence::Presence::full(blocks),
            };
            crate::info!(
                "disk {}: extent map rebuilt in {} ms (checkpoint + {n} segment metas); {} blocks absent",
                cfg.id,
                t0.elapsed().as_millis(),
                presence.absent()
            );
        }
        if !presence_path.exists() {
            // an explicit file, even when every block is local: an empty file would read as all absent
            write_synced(&presence_path, &presence.to_bytes())?;
        }
        let rep = cfg.replica.as_ref().map(|a| crate::replica::Replicator::new(a, &cfg.prefix, epoch));
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
            image_buf: OpenOptions::new().read(true).write(true).open(dir.join("data"))?,
            image,
            intent_file,
            regions: Mutex::new(Regions { t: tracker, cleared_unsynced: false, last_sync: Instant::now() }),
            pending_recovery: Mutex::new(dirty),
            open: Mutex::new(Builder::default()),
            open_since: AtomicU64::new(0),
            queue: Mutex::new(Queue::default()),
            cmap: Mutex::new(cmap),
            store,
            last_ckpt: AtomicU64::new(0),
            last_ckpt_user: AtomicU64::new(0),
            user_bytes: AtomicU64::new(0),
            last_ckpt_at: Mutex::new(Instant::now()),
            // the checkpoint this disk was opened from counts as the previous one for GC
            ckpts: Mutex::new(loaded_ckpt.into_iter().collect()),
            fully_present: AtomicBool::new(presence.absent() == 0),
            lazy: Mutex::new(LazyState { file_present: true, p: presence, dirty_pages: BTreeSet::new(), reading: Default::default() }),
            access: (0..size.div_ceil(REGION)).map(|_| AtomicU32::new(0)).collect(),
            rep,
            rep_cursor: Mutex::new((0, 0, 0)),
            evict_paused: AtomicBool::new(false),
            presence_file: OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&presence_path)?,
            metas: restore::MetaCache::default(),
            queue_cv: Condvar::new(),
            epoch,
            next_seq: AtomicU64::new(0),
            committed: AtomicU64::new(0),
            policy: AtomicU8::new(policy),
            fenced: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            stats: Stats::default(),
        })
    }

    /// Any absent block in `[off, off + len)`?
    pub fn has_absent(&self, off: u64, len: u64) -> bool {
        if self.fully_present.load(Acquire) {
            return false;
        }
        let bs = crate::extmap::BLOCK;
        let l = self.lazy.lock().unwrap();
        (off / bs..(off + len).div_ceil(bs)).any(|b| !l.p.is_present(b))
    }

    /// A discard over blocks that are still absent writes zeros to them instead of leaving holes, so
    /// that after a crash an absent block that is a hole in the image can only mean "never fetched"
    /// (see `recover`). Called between `write_begin` and `write_end`, so no fetch lands meanwhile.
    pub fn zero_absent(&self, off: u64, len: u64) -> Result<()> {
        if self.fully_present.load(Acquire) {
            return Ok(());
        }
        let bs = crate::extmap::BLOCK;
        let absent: Vec<u64> = {
            let l = self.lazy.lock().unwrap();
            (off / bs..(off + len).div_ceil(bs)).filter(|b| !l.p.is_present(*b)).collect()
        };
        let zeros = vec![0u8; bs as usize];
        for b in absent {
            self.image_buf.write_all_at(&zeros, b * bs)?;
        }
        Ok(())
    }

    /// A write or discard to `[off, off + len)` starts / ends (lazy bookkeeping; free once complete).
    pub fn write_begin(&self, off: u64, len: u64) {
        if !self.fully_present.load(Acquire) {
            let bs = crate::extmap::BLOCK;
            self.lazy.lock().unwrap().p.write_begin(off / bs, len.div_ceil(bs));
        }
    }
    pub fn write_end(&self, off: u64, len: u64, ok: bool) {
        if self.fully_present.load(Acquire) {
            return;
        }
        let bs = crate::extmap::BLOCK;
        let (b, n) = (off / bs, len.div_ceil(bs));
        let mut l = self.lazy.lock().unwrap();
        l.p.write_end(b, n, ok);
        if ok {
            for x in b..b + n {
                l.dirty_pages.insert((x / 8) as usize / PRESENCE_PAGE);
            }
        }
        if l.p.absent() == 0 {
            self.fully_present.store(true, Release);
        }
    }

    /// Fetch every fillable absent block in `[b, b + n)` from the object store into the image.
    pub fn ensure_present(&self, b: u64, n: u64) -> Result<u64> {
        // A segment can vanish between planning and fetching: a local write supersedes the block
        // and GC deletes the old segment. Planning again from the current map then skips it.
        let mut tries = 0;
        loop {
            match self.ensure_present_once(b, n) {
                Err(e) if tries < 3 => {
                    tries += 1;
                    crate::debug!("disk {}: fetch of blocks {b}+{n} failed, planning again: {e:#}", self.cfg.id);
                }
                r => return r,
            }
        }
    }

    fn ensure_present_once(&self, b: u64, n: u64) -> Result<u64> {
        let mut want = Vec::new();
        {
            let mut l = self.lazy.lock().unwrap();
            let map = self.cmap.lock().unwrap();
            for x in b..(b + n).min(map.blocks()) {
                if l.p.fillable(x) {
                    match map.get(x) {
                        Some((si, dblk)) => want.push((x, map.segs[si as usize], dblk)),
                        None => {
                            l.p.filled(x); // a hole: the empty image already reads zeros
                            l.dirty_pages.insert((x / 8) as usize / PRESENCE_PAGE);
                        }
                    }
                }
            }
        }
        if want.is_empty() {
            return Ok(0);
        }
        let got = restore::fetch_blocks(self.store.as_ref(), &self.cfg.prefix, &self.metas, &want).inspect_err(|_| {
            self.stats.fetch_errors.fetch_add(1, Relaxed);
        })?;
        let img = &self.image_buf; // buffered: fetched data is not aligned
        let mut l = self.lazy.lock().unwrap();
        let mut bytes = 0u64;
        for (x, data) in got {
            if l.p.fillable(x) {
                img.write_all_at(&data, x * crate::extmap::BLOCK)?;
                l.p.filled(x);
                l.dirty_pages.insert((x / 8) as usize / PRESENCE_PAGE);
                bytes += data.len() as u64;
            }
        }
        if l.p.absent() == 0 {
            self.fully_present.store(true, Release);
        }
        drop(l);
        self.stats.fetched_bytes.fetch_add(bytes, Relaxed);
        Ok(bytes)
    }

    /// Bitmap pages to persist after the image is fdatasync'd: taken *before* the fdatasync, so
    /// every block they mark present was in the image when the sync started.
    pub fn presence_snapshot(&self) -> Option<Vec<(usize, Vec<u8>)>> {
        let mut l = self.lazy.lock().unwrap();
        if !l.file_present || l.dirty_pages.is_empty() && l.p.absent() > 0 {
            return None;
        }
        let bytes = l.p.to_bytes();
        let pages = std::mem::take(&mut l.dirty_pages)
            .into_iter()
            .map(|pg| {
                let s = pg * PRESENCE_PAGE;
                (pg, bytes[s.min(bytes.len())..(s + PRESENCE_PAGE).min(bytes.len())].to_vec())
            })
            .collect();
        Some(pages)
    }

    /// Persist a snapshot after the image fdatasync. Once every block is present the file is removed.
    pub fn presence_persist(&self, pages: Vec<(usize, Vec<u8>)>) -> Result<()> {
        for (pg, b) in &pages {
            self.presence_file.write_all_at(b, (pg * PRESENCE_PAGE) as u64)?;
        }
        self.presence_file.sync_data()?;
        Ok(())
    }

    /// A read of `[off, off + len)` starts / ends; with a cache budget, eviction skips regions with
    /// reads in flight (a read that found its blocks present must find them still there).
    pub fn read_begin(&self, off: u64, len: u64) {
        self.touch(off, len);
        if self.cfg.cache_mb > 0 {
            let mut l = self.lazy.lock().unwrap();
            for r in off / REGION..=(off + len.max(1) - 1) / REGION {
                *l.reading.entry(r).or_insert(0) += 1;
            }
        }
    }
    pub fn read_end(&self, off: u64, len: u64) {
        if self.cfg.cache_mb > 0 {
            let mut l = self.lazy.lock().unwrap();
            for r in off / REGION..=(off + len.max(1) - 1) / REGION {
                if let Some(c) = l.reading.get_mut(&r) {
                    *c -= 1;
                    if *c == 0 {
                        l.reading.remove(&r);
                    }
                }
            }
        }
    }

    /// Record an access for the eviction order.
    pub fn touch(&self, off: u64, len: u64) {
        if self.cfg.cache_mb > 0 {
            let now = (mono_ms() / 1000) as u32;
            for r in off / REGION..=((off + len.max(1) - 1) / REGION).min(self.access.len() as u64 - 1) {
                self.access[r as usize].store(now, Relaxed);
            }
        }
    }

    /// Bytes of the local image that hold data (allocated blocks).
    pub fn cached_bytes(&self) -> u64 {
        self.image_buf.metadata().map(|m| std::os::unix::fs::MetadataExt::blocks(&m) * 512).unwrap_or(0)
    }

    /// Keep the local image within `cache_mb`: drop the least recently used regions whose writes are
    /// all committed (the object store has exactly what the image has), down to 90% of the budget.
    pub fn evictor(&self) {
        let budget = self.cfg.cache_mb << 20;
        if budget == 0 {
            return;
        }
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if self.fenced.load(SeqCst) || self.stopped.load(SeqCst) {
                return;
            }
            if self.evict_paused.load(SeqCst) || self.cached_bytes() <= budget {
                continue;
            }
            let mut order: Vec<usize> = (0..self.access.len()).collect();
            order.sort_by_key(|r| self.access[*r].load(Relaxed));
            for r in order {
                if self.cached_bytes() <= budget / 10 * 9 || self.stopped.load(SeqCst) {
                    break;
                }
                match self.evict_region(r) {
                    Ok(n) => {
                        self.stats.evicted_bytes.fetch_add(n, Relaxed);
                    }
                    Err(e) => {
                        crate::warn!("disk {}: evicting region {r} failed: {e:#}", self.cfg.id);
                        break;
                    }
                }
            }
        }
    }

    /// Drop the data of region `r` from the local image if all its writes are committed and no I/O
    /// touches it. Locks, in order: open segment (no repack copy reads the image meanwhile), regions
    /// (no write starts), lazy (no read starts, no fetch fills). The bitmap is persisted with the
    /// blocks absent before the hole is punched, so it never claims a punched block present.
    fn evict_region(&self, r: usize) -> Result<u64> {
        let bs = crate::extmap::BLOCK;
        let (start, end) = (r as u64 * REGION, ((r as u64 + 1) * REGION).min(self.size));
        let _o = self.open.lock().unwrap();
        let g = self.regions.lock().unwrap();
        if !g.t.clean(r) {
            return Ok(0);
        }
        let mut l = self.lazy.lock().unwrap();
        if l.reading.contains_key(&(r as u64)) {
            return Ok(0);
        }
        let fd = self.image_buf.as_raw_fd();
        let mut extents = Vec::new();
        let mut pos = start;
        while pos < end {
            let d = unsafe { libc::lseek(fd, pos as i64, libc::SEEK_DATA) };
            if d < 0 || d as u64 >= end {
                break;
            }
            let h = unsafe { libc::lseek(fd, d, libc::SEEK_HOLE) };
            let dend = if h < 0 { end } else { (h as u64).min(end) };
            extents.push((d as u64, dend));
            pos = dend;
        }
        if extents.is_empty() {
            return Ok(0);
        }
        let map = self.cmap.lock().unwrap();
        let mut gone = Vec::new();
        for &(a, z) in &extents {
            for b in a / bs..z.div_ceil(bs) {
                // only blocks the store holds; a block the map does not point at reads as zeros
                if map.get(b).is_some() && l.p.is_present(b) {
                    gone.push(b);
                }
            }
        }
        drop(map);
        if gone.is_empty() {
            return Ok(0);
        }
        self.image_buf.sync_data()?; // every bit the pages below claim present is durable
        for &b in &gone {
            if l.p.evict(b) {
                l.dirty_pages.insert((b / 8) as usize / PRESENCE_PAGE);
            }
        }
        let bytes = l.p.to_bytes();
        let pages: BTreeSet<usize> = gone.iter().map(|b| (b / 8) as usize / PRESENCE_PAGE).collect();
        for pg in pages {
            let s = pg * PRESENCE_PAGE;
            self.presence_file.write_all_at(&bytes[s.min(bytes.len())..(s + PRESENCE_PAGE).min(bytes.len())], s as u64)?;
        }
        self.presence_file.sync_data()?;
        self.fully_present.store(false, Release);
        let mut freed = 0;
        let mut i = 0;
        while i < gone.len() {
            let mut j = i + 1;
            while j < gone.len() && gone[j] == gone[j - 1] + 1 {
                j += 1;
            }
            log::punch(fd, gone[i] * bs, (j - i) as u64 * bs)?;
            freed += (j - i) as u64 * bs;
            i = j;
        }
        drop(l);
        drop(g);
        Ok(freed)
    }

    /// Background hydration after a lazy restore, at `hydrate_mbps`.
    pub fn hydrator(&self) {
        let blocks = self.size.div_ceil(crate::extmap::BLOCK);
        let chunk = 256u64; // 1 MiB of blocks per step
        let mut next = 0u64;
        // with a cache budget, blocks come back only when read
        while self.cfg.hydrate_mbps > 0
            && self.cfg.cache_mb == 0
            && !self.fully_present.load(Acquire)
            && !self.fenced.load(SeqCst)
            && !self.stopped.load(SeqCst)
        {
            let start = { self.lazy.lock().unwrap().p.next_absent(next, blocks) };
            let Some(b) = start.or_else(|| self.lazy.lock().unwrap().p.next_absent(0, blocks)) else { break };
            let t0 = Instant::now();
            match self.ensure_present(b, chunk.min(blocks - b)) {
                Ok(bytes) => {
                    next = b + chunk;
                    let budget = std::time::Duration::from_secs_f64(bytes as f64 / (self.cfg.hydrate_mbps as f64 * 1048576.0));
                    if let Some(rest) = budget.checked_sub(t0.elapsed()) {
                        std::thread::sleep(rest);
                    }
                }
                Err(e) => {
                    crate::warn!("disk {}: hydration fetch failed (will retry): {e:#}", self.cfg.id);
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
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
            gc_deleted_segments: s.gc_deleted_segments.load(Relaxed),
            gc_deleted_bytes: s.gc_deleted_bytes.load(Relaxed),
            gc_repacked_bytes: s.gc_repacked_bytes.load(Relaxed),
            absent_blocks: self.lazy.lock().unwrap().p.absent(),
            fetched_bytes: s.fetched_bytes.load(Relaxed),
            fetch_errors: s.fetch_errors.load(Relaxed),
            cached_bytes: self.cached_bytes(),
            evicted_bytes: s.evicted_bytes.load(Relaxed),
        }
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
        if o.is_empty() {
            self.open_since.store(mono_ms(), SeqCst);
        }
        self.user_bytes.fetch_add(d.len() as u64, SeqCst);
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
        if o.is_empty() {
            self.open_since.store(mono_ms(), SeqCst);
        }
        o.push_discard(off, len);
        self.next_seq.load(SeqCst)
    }

    /// Stop this disk's background threads (dispatcher, checkpointer, hydrator): it was detached.
    pub fn stop(&self) {
        self.stopped.store(true, SeqCst);
        let _q = self.queue.lock().unwrap();
        self.queue_cv.notify_all();
    }

    /// Does a flush have to seal? Under `remote` it waits for the upload, so yes; under `local` the
    /// open segment is sealed by `seal_if_due` instead, unless that is disabled.
    pub fn flush_seals(&self) -> bool {
        self.policy.load(SeqCst) == POLICY_REMOTE || self.cfg.seal_interval_ms == 0
    }

    /// Seal the open segment once its oldest write is `seal_interval_ms` old (called periodically).
    pub fn seal_if_due(&self) {
        let since = self.open_since.load(SeqCst);
        if self.cfg.seal_interval_ms > 0 && since != 0 && mono_ms() - since >= self.cfg.seal_interval_ms {
            self.seal();
        }
    }

    /// Close the open segment (if non-empty) and queue it. Returns the `committed` value that covers
    /// everything appended so far.
    pub fn seal(&self) -> u64 {
        let mut o = self.open.lock().unwrap();
        if o.is_empty() {
            return self.next_seq.load(SeqCst);
        }
        let seq = self.next_seq.fetch_add(1, SeqCst);
        if let Some(rep) = &self.rep {
            self.rep_send_delta(rep, &o, seq);
            rep.seal(seq);
            *self.rep_cursor.lock().unwrap() = (seq + 1, 0, 0);
        }
        let seg = std::mem::take(&mut *o);
        self.open_since.store(0, SeqCst);
        let mut q = self.queue.lock().unwrap();
        q.bytes += seg.size() as u64;
        q.sealed.push_back(Sealed { seq, seg: Arc::new(seg) });
        drop(q);
        drop(o);
        self.queue_cv.notify_all();
        seq + 1
    }

    /// Queue for the standby what of the open segment (seq `seq`) it does not have yet. Called with
    /// the open-segment lock held.
    fn rep_send_delta(&self, rep: &crate::replica::Replicator, o: &Builder, seq: u64) {
        let mut c = self.rep_cursor.lock().unwrap();
        if c.0 != seq {
            *c = (seq, 0, 0);
        }
        if o.entries.len() > c.1 {
            let body = crate::replica::encode_delta(o, c.1);
            match self.store.seal_blob(&crate::replica::batch_name(&self.cfg.prefix, self.epoch, seq, c.2), body) {
                Ok(b) => {
                    rep.append(seq, c.2, b);
                    *c = (seq, o.entries.len(), c.2 + 1);
                }
                Err(e) => crate::warn!("disk {}: sealing a journal batch failed: {e:#}", self.cfg.id),
            }
        }
    }

    /// At a flush under `local` with a standby: queue what it lacks and a barrier. Returns the
    /// barrier to wait for, or None when the standby is unavailable (then the flush must wait for
    /// the object store instead).
    pub fn rep_flush(&self) -> Option<u64> {
        let rep = self.rep.as_ref()?;
        let o = self.open.lock().unwrap();
        let seq = self.next_seq.load(SeqCst);
        self.rep_send_delta(rep, &o, seq);
        rep.barrier()
    }

    /// After a (re)connection to the standby: the open segment in full, and the cursor at its end.
    pub fn rep_resync(&self, emit: &mut dyn FnMut(u64, u32, Vec<u8>)) -> u64 {
        let o = self.open.lock().unwrap();
        let seq = self.next_seq.load(SeqCst);
        let mut c = self.rep_cursor.lock().unwrap();
        *c = (seq, 0, 0);
        if !o.is_empty() {
            let body = crate::replica::encode_delta(&o, 0);
            if let Ok(b) = self.store.seal_blob(&crate::replica::batch_name(&self.cfg.prefix, self.epoch, seq, 0), body) {
                emit(seq, 0, b);
                *c = (seq, o.entries.len(), 1);
            }
        }
        if let Some(rep) = &self.rep {
            rep.go_live(seq);
        }
        seq
    }

    pub fn stopped_or_fenced(&self) -> bool {
        self.stopped.load(SeqCst) || self.fenced.load(SeqCst)
    }

    /// Re-upload what a crashed run left behind: the data extents of each dirty region (holes become
    /// discards, found with SEEK_DATA/SEEK_HOLE). Builds the segment before the device is attached;
    /// the upload proceeds in the background, ahead of every later segment.
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
        let rd = &self.image_buf; // buffered: no O_DIRECT alignment rules
        let fd = rd.as_raw_fd();
        let (mut data_bytes, mut holes) = (0u64, 0u64);
        let mut buf = vec![0u8; 1 << 20];
        let mut o = self.open.lock().unwrap();
        let seq = self.next_seq.load(SeqCst); // the open segment is empty: the device is not attached yet
        let mut ranges = Vec::with_capacity(regions.len());
        let lazy = !self.fully_present.load(Acquire);
        for r in &regions {
            let (start, end) = (r * REGION, ((r + 1) * REGION).min(self.size));
            ranges.push(self.intent_begin(start, end - start)?);
            // After a lazy restore the presence bitmap may be stale: it is persisted at flush, so
            // blocks written since then can be marked absent although their data is in the image.
            // Skipping them would upload later writes without earlier ones. Instead:
            // - a data extent in the image is never older than the block's committed version (it
            //   was fetched from it, or written since), so it is uploaded and becomes present;
            // - a hole that is present is a discard (or a block that was always zero);
            // - a hole that is absent was never fetched: discards write zeros over absent blocks
            //   (`zero_absent`), so it cannot be a discard. Its data lives in the store; skip it.
            let mut pos = start;
            while pos < end {
                let d = unsafe { libc::lseek(fd, pos as i64, libc::SEEK_DATA) };
                let dstart = if d < 0 { end } else { (d as u64).min(end) };
                if dstart > pos && !lazy {
                    o.push_discard(pos, (dstart - pos) as u32);
                    holes += 1;
                } else if dstart > pos {
                    let bs = crate::extmap::BLOCK;
                    let l = self.lazy.lock().unwrap();
                    let mut b = pos / bs;
                    while b < dstart / bs {
                        if !l.p.is_present(b) {
                            b += 1;
                            continue;
                        }
                        let s = b;
                        while b < dstart / bs && l.p.is_present(b) {
                            b += 1;
                        }
                        o.push_discard(s * bs, ((b - s) * bs) as u32);
                        holes += 1;
                    }
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
                if lazy {
                    let bs = crate::extmap::BLOCK;
                    let mut l = self.lazy.lock().unwrap();
                    for b in dstart / bs..dend.div_ceil(bs) {
                        if !l.p.is_present(b) {
                            l.p.filled(b);
                            l.dirty_pages.insert((b / 8) as usize / PRESENCE_PAGE);
                        }
                    }
                }
                pos = dend;
            }
        }
        drop(o);
        if lazy {
            // blocks found present above: persist them (image first, so the bitmap never claims
            // data that is not durable)
            let snap = self.presence_snapshot();
            rd.sync_data()?;
            if let Some(pages) = snap {
                self.presence_persist(pages)?;
            }
            if self.lazy.lock().unwrap().p.absent() == 0 {
                self.fully_present.store(true, Release);
            }
        }
        // The segment had to be built before the device is attached (new writes must not change the
        // image under it), but it need not be uploaded yet: it is sealed first, so every later
        // segment commits after it, and the intent bits stay set until it commits. Waiting for the
        // upload here would hold the device for as long as the dirty data takes to upload, past
        // the supervisor's orphan timeout on a slow link.
        self.seal();
        for range in ranges {
            self.intent_end(range, Some(seq));
        }
        crate::info!(
            "disk {}: recovery queued {} regions ({} MiB data, {holes} holes) as one segment in {} ms",
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
        let (bytes, c, referenced) = {
            let map = self.cmap.lock().unwrap();
            let c = self.committed.load(SeqCst);
            if c == 0 || c == self.last_ckpt.load(SeqCst) || self.fenced.load(SeqCst) {
                return Ok(false);
            }
            (map.encode_checkpoint(self.epoch, c - 1), c, map.referenced())
        };
        let key = log::key_ckpt(&self.cfg.prefix, self.epoch, c - 1);
        self.store.put_new(&key, &bytes)?; // Exists: an earlier attempt landed; same content
        let mut ck = self.ckpts.lock().unwrap();
        ck.push_back(crate::gc::Checkpoint { pos: (self.epoch, c - 1), referenced });
        while ck.len() > 2 {
            ck.pop_front();
        }
        drop(ck);
        self.last_ckpt.store(c, SeqCst);
        self.last_ckpt_user.store(self.user_bytes.load(SeqCst), SeqCst);
        *self.last_ckpt_at.lock().unwrap() = Instant::now();
        crate::debug!("disk {}: checkpoint {key} ({} bytes)", self.cfg.id, bytes.len());
        Ok(true)
    }

    /// Checkpoint every CKPT_EVERY committed segments, or every CKPT_INTERVAL_SECS if anything committed.
    pub fn checkpointer(&self) {
        loop {
            std::thread::sleep(std::time::Duration::from_millis(250));
            if self.fenced.load(SeqCst) || self.stopped.load(SeqCst) {
                return;
            }
            let (c, last) = (self.committed.load(SeqCst), self.last_ckpt.load(SeqCst));
            // by segments, by bytes or by time: GC keeps everything above the older of the last two
            // checkpoints, so the store holds up to two intervals of segments; with large (batched)
            // segments only a byte limit keeps that bounded. Only data written by users counts:
            // repack copies made by GC would otherwise trigger the next round themselves.
            let uploaded = self.user_bytes.load(SeqCst) - self.last_ckpt_user.load(SeqCst);
            let due = c >= last + self.cfg.checkpoint_every.max(1)
                || (c > last && uploaded >= self.cfg.checkpoint_mb.max(1) << 20)
                || (c > last && self.last_ckpt_at.lock().unwrap().elapsed().as_secs() >= CKPT_INTERVAL_SECS);
            if due {
                match self.checkpoint() {
                    Ok(true) if self.cfg.gc => {
                        if let Err(e) = self.gc_round() {
                            crate::warn!("disk {}: gc failed (will retry): {e:#}", self.cfg.id);
                        }
                    }
                    Ok(_) => {}
                    Err(e) => crate::warn!("disk {}: checkpoint failed (will retry): {e:#}", self.cfg.id),
                }
            }
        }
    }

    /// One GC round against the last two checkpoints: delete segments neither references (at or below
    /// the older one), delete checkpoints older than it, and repack sparse segments.
    /// Returns (segments deleted, bytes repacked).
    pub fn gc_round(&self) -> Result<(usize, u64)> {
        let (older, newer) = {
            let ck = self.ckpts.lock().unwrap();
            if ck.len() < 2 || self.fenced.load(SeqCst) {
                return Ok((0, 0));
            }
            (
                crate::gc::Checkpoint { pos: ck[0].pos, referenced: ck[0].referenced.clone() },
                crate::gc::Checkpoint { pos: ck[1].pos, referenced: ck[1].referenced.clone() },
            )
        };
        let p = &self.cfg.prefix;
        // a newer epoch means another server owns the disk: it alone may delete from now on
        if self.store.list(&format!("{p}/epoch/"))?.iter().filter_map(|k| log::parse_epoch_key(k)).any(|e| e > self.epoch) {
            self.fenced.store(true, SeqCst);
            crate::error!("disk {}: FENCED: a newer epoch exists; stopping GC and failing all I/O", self.cfg.id);
            return Ok((0, 0));
        }
        // checkpoints first: while a checkpoint exists, every segment it references exists, so a
        // reader holding a checkpoint (or a copy of the store that contains one) is never left with
        // a hole in the log
        for k in crate::gc::deletable_checkpoints(&self.store.list(&format!("{p}/ckpt/"))?, older.pos) {
            self.store.delete(&k)?;
        }
        let listing = self.store.list_sized(&format!("{p}/seg/"))?;
        let doomed = crate::gc::deletable_segments(&listing, &older, &newer);
        let sizes: std::collections::HashMap<&str, u64> = listing.iter().map(|(k, s)| (k.as_str(), *s)).collect();
        for k in &doomed {
            self.store.delete(k)?;
            self.stats.gc_deleted_segments.fetch_add(1, Relaxed);
            self.stats.gc_deleted_bytes.fetch_add(sizes.get(k.as_str()).copied().unwrap_or(0), Relaxed);
        }
        let repacked = self.repack(newer.pos)?;
        if !doomed.is_empty() || repacked > 0 {
            crate::info!("disk {}: gc deleted {} segments, repacked {} KiB", self.cfg.id, doomed.len(), repacked >> 10);
        }
        Ok((doomed.len(), repacked))
    }

    /// Rewrite the live blocks of sparse segments (at or below `upto`) into the open segment, from the
    /// local image. Done under the open-segment lock, so no write can append between reading a
    /// block and appending its copy: the copy is never older than a write logged before it.
    fn repack(&self, upto: (u64, u64)) -> Result<u64> {
        let (victims, runs) = {
            let map = self.cmap.lock().unwrap();
            let live = map.live_blocks();
            let budget = (self.cfg.repack_budget_mb << 20) / crate::extmap::BLOCK;
            let v = crate::gc::repack_candidates(&live, &map.segs, &map.data_blocks, upto, self.cfg.repack_ratio, budget);
            let set: BTreeSet<u32> = v.iter().map(|(i, _)| *i).collect();
            let runs: Vec<(u64, u32)> = map.runs().into_iter().filter(|(_, _, s, _)| set.contains(s)).map(|(b, n, _, _)| (b, n)).collect();
            (v, runs)
        };
        if victims.is_empty() {
            return Ok(0);
        }
        let runs: Vec<(u64, u32)> = if self.fully_present.load(Acquire) {
            runs
        } else {
            // copy only what the image holds; absent runs stay in their segment for now
            let l = self.lazy.lock().unwrap();
            runs.into_iter().filter(|(b, n)| (*b..*b + *n as u64).all(|x| l.p.is_present(x))).collect()
        };
        let bytes = self.copy_runs(&runs)?;
        self.stats.gc_repacked_bytes.fetch_add(bytes, Relaxed);
        Ok(bytes)
    }

    /// Copy runs `(block, count)` from the local image into the open segment (under its lock, so no
    /// write can append between reading a block and appending its copy), then seal.
    fn copy_runs(&self, runs: &[(u64, u32)]) -> Result<u64> {
        let rd = &self.image_buf;
        let mut bytes = 0u64;
        let mut buf = Vec::new();
        for &(b, n) in runs {
            let (off, len) = (b * crate::extmap::BLOCK, n as u64 * crate::extmap::BLOCK);
            let mut o = self.open.lock().unwrap();
            // under the open-segment lock nothing is evicted; skip a run evicted since it was chosen
            if !self.fully_present.load(Acquire) {
                let l = self.lazy.lock().unwrap();
                if !(b..b + n as u64).all(|x| l.p.is_present(x)) {
                    continue;
                }
            }
            let range = self.intent_begin(off, len)?;
            buf.resize(len as usize, 0);
            rd.read_exact_at(&mut buf, off)?;
            if o.is_empty() {
                self.open_since.store(mono_ms(), SeqCst);
            }
            o.push_data(off, &buf);
            let seq = self.next_seq.load(SeqCst);
            let full = o.data_len() >= MAX_SEG;
            drop(o);
            self.intent_end(range, Some(seq));
            bytes += len;
            if full {
                self.seal();
            }
        }
        self.seal();
        Ok(bytes)
    }

    /// Seal and wait until everything so far is committed.
    fn drain_all(&self) -> Result<()> {
        let need = self.seal();
        let mut q = self.queue.lock().unwrap();
        while self.committed.load(SeqCst) < need {
            anyhow::ensure!(!self.fenced.load(SeqCst), "disk {} is fenced", self.cfg.id);
            q = self.queue_cv.wait_timeout(q, std::time::Duration::from_millis(100)).unwrap().0;
        }
        Ok(())
    }

    /// After a key rotation (the disk opened with a new `key_file` and the old key in
    /// `old_key_files`), rewrite every live block still held by a segment of an earlier epoch,
    /// which may be sealed with an old key, then checkpoint twice so GC deletes those segments and
    /// the old checkpoints. Finally list what is left and check it opens with the current key.
    /// Tombstones and epoch markers are never read, so they may keep an old key.
    pub fn rekey(&self) -> Result<String> {
        self.evict_paused.store(true, SeqCst);
        let r = self.rekey_inner();
        self.evict_paused.store(false, SeqCst);
        r
    }

    fn rekey_inner(&self) -> Result<String> {
        let t0 = Instant::now();
        let bs = crate::extmap::BLOCK;
        let blocks = self.size.div_ceil(bs);
        let mut b = 0;
        while !self.fully_present.load(Acquire) && b < blocks {
            self.ensure_present(b, 256.min(blocks - b))?; // every block local, so it can be copied
            b += 256;
        }
        let mut rewritten = 0u64;
        loop {
            let runs: Vec<(u64, u32)> = {
                let map = self.cmap.lock().unwrap();
                let mut take = 0u64;
                map.runs()
                    .into_iter()
                    .filter(|(_, _, si, _)| map.segs[*si as usize].0 < self.epoch)
                    .take_while(|(_, n, _, _)| {
                        take += *n as u64 * bs;
                        take <= 64 << 20 || take == *n as u64 * bs
                    })
                    .map(|(b, n, _, _)| (b, n))
                    .collect()
            };
            if runs.is_empty() {
                break;
            }
            rewritten += self.copy_runs(&runs)?;
            self.drain_all()?;
        }
        for _ in 0..2 {
            self.drain_all()?;
            if !self.checkpoint()? {
                // nothing new to checkpoint: commit a copy of one run so the position advances
                let first = { self.cmap.lock().unwrap().runs().into_iter().next().map(|(b, n, _, _)| (b, n.min(1))) };
                if let Some(r) = first {
                    self.copy_runs(&[r])?;
                    self.drain_all()?;
                    self.checkpoint()?;
                }
            }
            self.gc_round()?;
        }
        let p = &self.cfg.prefix;
        let mut stale = Vec::new();
        let segs = self.store.list_sized(&format!("{p}/seg/"))?;
        let objs =
            segs.into_iter().filter(|(_, s)| *s as usize != log::TOMB.len()).map(|(k, _)| k).chain(self.store.list(&format!("{p}/ckpt/"))?);
        for k in objs {
            if self.store.needs_old_key(&k)? {
                stale.push(k);
            }
        }
        let head = format!("disk {}: rewrote {} MiB in {} ms", self.cfg.id, rewritten >> 20, t0.elapsed().as_millis());
        Ok(if stale.is_empty() {
            format!("ok {head}; no segment or checkpoint needs an old key any more")
        } else {
            format!("ok {head}; {} objects still need an old key (e.g. {}); run rekey again", stale.len(), stale[0])
        })
    }

    /// Hands sealed segments to the shared pool in seq order, at most WINDOW beyond the committed prefix.
    pub fn dispatcher(self: &Arc<Self>, pool: &Pool) {
        loop {
            let (seq, raw) = {
                let mut q = self.queue.lock().unwrap();
                loop {
                    if self.fenced.load(SeqCst) || self.stopped.load(SeqCst) {
                        return;
                    }
                    let committed = self.committed.load(SeqCst);
                    let next = q.sealed.iter().find(|s| s.seq >= q.dispatched).map(|s| (s.seq, s.seg.clone()));
                    match next {
                        // with a standby, a segment uploads only once the standby has all of it
                        Some((seq, raw)) if seq < committed + WINDOW && self.rep.as_ref().is_none_or(|r| r.may_upload(seq)) => {
                            q.dispatched = seq + 1;
                            break (seq, raw);
                        }
                        _ if self.rep.is_some() => q = self.queue_cv.wait_timeout(q, std::time::Duration::from_millis(20)).unwrap().0,
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
                    if let Some(rep) = &self.rep {
                        rep.committed(q.commits.committed());
                    }
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
            seal_epochs(s3, p, last)?;
        }
        return Ok(last + 1);
    }
}

/// Upload from the standby's journal what the object store lacks of earlier epochs (a host that
/// died with acknowledged writes not yet uploaded), before this server claims its epoch. If the
/// standby is unreachable, continue only when this host wrote the disk last (its local image has
/// everything, and recovery re-uploads it).
fn take_over_journal(s3: &dyn Store, p: &str, addr: &str, local_epoch: u64) -> Result<()> {
    let items = match crate::replica::fetch(addr, p) {
        Ok(items) => items,
        Err(e) => {
            let last = s3.list(&format!("{p}/epoch/"))?.iter().filter_map(|k| log::parse_epoch_key(k)).max().unwrap_or(0);
            if local_epoch != 0 && local_epoch == last {
                crate::warn!("replica {addr} unreachable ({e:#}); this host wrote the disk last, continuing from its local image");
                return Ok(());
            }
            bail!("replica {addr} unreachable: {e:#}. The last acknowledged writes may be only there; remove \"replica\" from the disk to open it without them");
        }
    };
    if items.is_empty() {
        return Ok(());
    }
    type Batches = Vec<(u32, Vec<u8>)>;
    let mut segs: BTreeMap<(u64, u64), (bool, Batches)> = BTreeMap::new();
    for it in items {
        let e = segs.entry((it.epoch, it.seq)).or_insert((it.sealed, Vec::new()));
        e.0 |= it.sealed;
        e.1.push((it.n, it.body));
    }
    let epochs: BTreeSet<u64> = segs.keys().map(|(e, _)| *e).collect();
    let mut uploaded = 0;
    for &e in &epochs {
        let objs: std::collections::BTreeMap<u64, bool> = s3
            .list_sized(&format!("{p}/seg/{e:010}/"))?
            .into_iter()
            .filter_map(|(k, size)| log::parse_seg_key(&k).map(|(_, s)| (s, size as usize == log::TOMB.len())))
            .collect();
        if log::is_sealed(&objs) {
            continue; // a later writer fenced this epoch already
        }
        let store_max = objs.keys().next_back().copied();
        let journal_max = segs.range((e, 0)..=(e, u64::MAX)).map(|((_, s), _)| *s).max().unwrap_or(0);
        for ((_, seq), (sealed, batches)) in segs.range((e, 0)..=(e, u64::MAX)) {
            if objs.contains_key(seq) {
                continue;
            }
            // an unsealed segment ends at the last acknowledged flush: usable only as the very last
            if !sealed && !(*seq == journal_max && store_max.is_none_or(|m| m < *seq)) {
                continue;
            }
            let mut b = Builder::default();
            for (n, body) in batches {
                let plain = s3.open_blob(&crate::replica::batch_name(p, e, *seq, *n), body.clone())?;
                crate::replica::apply_delta(&mut b, &plain)?;
            }
            if b.is_empty() {
                continue;
            }
            // `committed` 0: a rebuilt segment attests nothing about the rest of the log
            s3.put_new(&log::key_seg(p, e, *seq), &segment::encode(e, *seq, 0, &b))?;
            uploaded += 1;
        }
    }
    if uploaded > 0 {
        crate::info!("{p}: uploaded {uploaded} segments from the replica's journal");
    }
    crate::replica::drop_below(addr, p, epochs.iter().max().copied().unwrap_or(0) + 1)?;
    Ok(())
}

/// Before claiming an epoch (which fences whoever serves the disk now), check that this server can
/// read the log at all: the newest checkpoint, or else the newest segment. A wrong or missing key
/// must fail here, not after the live writer was fenced out by a server that cannot open the disk.
fn probe_readable(s3: &dyn Store, p: &str) -> Result<()> {
    let newest_ckpt = s3.list(&format!("{p}/ckpt/"))?.into_iter().filter(|k| log::parse_ckpt_key(k).is_some()).max();
    let probe = match newest_ckpt {
        Some(k) => Some(k),
        None => s3
            .list_sized(&format!("{p}/seg/"))?
            .into_iter()
            .filter(|(k, size)| *size as usize != log::TOMB.len() && log::parse_seg_key(k).is_some())
            .map(|(k, _)| k)
            .max(),
    };
    if let Some(k) = probe {
        let head = s3.get_range(&k, 0, 64).with_context(|| k.clone())?;
        if k.contains("/seg/") {
            segment::meta_end(&head).with_context(|| k.clone())?;
        } else {
            anyhow::ensure!(!crate::crypt::looks_encrypted(&head), "{k}: the checkpoint is encrypted, but this disk has no key_file");
        }
    }
    Ok(())
}

/// Fence every unsealed epoch up to `last`, oldest first, so that afterwards every epoch below the
/// new one is sealed (its highest object is a tombstone). Sealing only the previous epoch is not
/// enough: a claim that crashed between writing its epoch marker and fencing would leave the epoch
/// before it unfenced, and that writer could keep landing segments that a later restore replays.
/// Oldest first, so a claim that crashes midway leaves a sealed prefix for the next claim to extend.
fn seal_epochs(s3: &dyn Store, p: &str, last: u64) -> Result<()> {
    let mut objs: std::collections::BTreeMap<u64, std::collections::BTreeMap<u64, bool>> = Default::default();
    for (k, size) in s3.list_sized(&format!("{p}/seg/"))? {
        if let Some((e, s)) = log::parse_seg_key(&k) {
            objs.entry(e).or_default().insert(s, size as usize == log::TOMB.len());
        }
    }
    let mut ckpt_seq: std::collections::HashMap<u64, u64> = Default::default();
    for k in s3.list(&format!("{p}/ckpt/"))? {
        if let Some((e, s)) = log::parse_ckpt_key(&k) {
            let c = ckpt_seq.entry(e).or_insert(s);
            *c = (*c).max(s);
        }
    }
    let first = (1..=last).rev().find(|e| objs.get(e).is_some_and(log::is_sealed)).map_or(1, |e| e + 1);
    for e in first..=last {
        let existing: BTreeSet<u64> = objs.get(&e).map(|m| m.keys().copied().collect()).unwrap_or_default();
        let from = ckpt_seq.get(&e).map_or(0, |s| s + 1);
        // A gap is either an upload still in flight (fill it with a tombstone) or a lost segment.
        // Segments above the first gap attest how far the log was committed when they were
        // encoded; if one says the gap was already committed, the segment was lost: refuse,
        // rather than seal the hole and restore a partial log.
        let gap = (from..).find(|i| !existing.contains(i)).unwrap();
        for (&s, &tomb) in objs.get(&e).into_iter().flatten().filter(|(s, _)| **s > gap) {
            if tomb {
                continue;
            }
            let k = log::key_seg(p, e, s);
            let c = segment::committed(&s3.get_range(&k, 0, segment::HEADER as u64)?).with_context(|| k.clone())?;
            if c > gap {
                bail!(
                    "log incomplete: segment {} is missing, but {k} attests that the log was committed up to seq {}",
                    log::key_seg(p, e, gap),
                    c - 1
                );
            }
        }
        for sq in log::fence_targets(&existing, FENCE_SPAN, from) {
            // Exists = the old writer landed this one meanwhile; replay stops at the first tombstone anyway
            s3.put_new(&log::key_seg(p, e, sq), log::TOMB)?;
        }
    }
    Ok(())
}

/// Bits set in `full` and not in `sub` (both little-endian u64 bitmaps of equal length).
fn invert(sub: &[u8], full: &[u8]) -> Vec<u8> {
    full.iter().zip(sub).map(|(f, s)| f & !s).collect()
}

fn write_synced(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    let f = OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
    f.write_all_at(bytes, 0)?;
    f.sync_all()?;
    Ok(())
}

/// Fetch jobs from read requests that touch absent blocks (served off the ublk queue threads).
/// (disk, offset, length, eventfd to signal, failure flag)
type FetchJob = (Arc<Dev>, u64, u64, RawFd, Arc<AtomicU64>);

pub struct FetchPool {
    jobs: Mutex<VecDeque<FetchJob>>,
    cv: Condvar,
}

impl FetchPool {
    pub fn new() -> Arc<FetchPool> {
        Arc::new(FetchPool { jobs: Mutex::new(VecDeque::new()), cv: Condvar::new() })
    }
    /// Queue a fetch of `[off, off + len)`; `efd` is signalled when done, `failed` set to 1 on error.
    pub fn submit(&self, d: Arc<Dev>, off: u64, len: u64, efd: RawFd, failed: Arc<AtomicU64>) {
        self.jobs.lock().unwrap().push_back((d, off, len, efd, failed));
        self.cv.notify_one();
    }
    pub fn worker(&self) {
        loop {
            let (d, off, len, efd, failed) = {
                let mut j = self.jobs.lock().unwrap();
                while j.is_empty() {
                    j = self.cv.wait(j).unwrap();
                }
                j.pop_front().unwrap()
            };
            let bs = crate::extmap::BLOCK;
            if let Err(e) = d.ensure_present(off / bs, (off + len).div_ceil(bs) - off / bs) {
                crate::warn!("disk {}: on-demand fetch failed: {e:#}", d.cfg.id);
                failed.store(1, SeqCst);
            }
            signal(efd);
        }
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
    pub fn worker(&self) {
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
            // attest the committed prefix as of now: every seq below it is durable in the store
            let obj = segment::encode(dev.epoch, seq, dev.committed.load(SeqCst), &seg);
            let key = log::key_seg(&dev.cfg.prefix, dev.epoch, seq);
            let mut backoff = 100;
            let t0 = Instant::now();
            let outcome = loop {
                if dev.fenced.load(SeqCst) {
                    break PutOutcome::Exists;
                }
                match dev.store.put_new(&key, &obj) {
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
