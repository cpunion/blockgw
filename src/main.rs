//! blockgw: one process serves every virtual disk on this host through ublk; each disk lives in a
//! local sparse image and persists as a log of segments in S3-compatible object storage.
//!
//!   blockgw serve CONFIG.json [--orphan-timeout SECS] [--no-supervisor]
//!       serve every configured disk; devices left by a crashed server are re-attached, others added
//!   blockgw ctl SOCKET <command...>          policy <id> local|remote | lag <id> <MiB> | drain <id> | stats
//!                                            | attach <disk JSON> | detach <id> [purge]
//!   blockgw del <ublk-id>                    stop and delete a ublk device
//!
//! Data path per I/O (queue thread, io_uring shared with the ublk queue):
//!   READ  -> read from the local sparse data file (O_DIRECT)
//!   WRITE -> intent bit durable (first touch of a 4 MiB region only) -> write data file -> copy into open segment
//!   FLUSH -> fdatasync data file -> seal open segment -> policy wait (remote: uploaded; local: lag <= max_lag)
//!   DISCARD / WRITE_ZEROES -> punch hole -> record in open segment

mod crypt;
mod dev;
mod extmap;
mod failpoint;
mod gc;
mod log;
mod logx;
mod metrics;
mod presence;
mod restore;
mod s3;
mod segment;
mod store;
mod supervise;
mod track;

use anyhow::{Context, Result};
use dev::{Dev, DevConfig, POLICY_LOCAL, POLICY_REMOTE};
use io_uring::{opcode, types};
use libublk::helpers::IoBuf;
use libublk::io::{UblkDev, UblkQueue};
use libublk::uring_async::ublk_submit_sqe_async;
use libublk::{ctrl::UblkCtrl, BufDesc, UblkError, UblkFlags};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::Ordering::*;
use std::sync::{Arc, Mutex};

#[derive(serde::Deserialize)]
struct Config {
    /// S3-compatible object store for the segment log...
    s3: Option<s3::S3Config>,
    /// ...or a local directory (development, tests, or a second disk / NAS mount)
    local_store: Option<String>,
    /// optional "host:port" for a Prometheus `GET /metrics` endpoint
    metrics_addr: Option<String>,
    control_socket: String,
    #[serde(default)]
    devices: Vec<DevConfig>,
    /// optional directory of `<id>.json` disk files: disks attached and detached at runtime with
    /// `ctl attach` / `ctl detach`, re-attached when the worker restarts
    devices_dir: Option<String>,
    upload_workers: Option<usize>,
    /// ids of the disks that came from `devices_dir`
    #[serde(skip)]
    dynamic: HashSet<i32>,
}

/// What `ctl attach` needs to open a disk like the ones opened at startup.
struct Ctx {
    store: Arc<dyn store::Store>,
    pool: Arc<dev::Pool>,
    devices_dir: Option<String>,
    dynamic: Mutex<HashSet<i32>>,
}
static CTX: std::sync::OnceLock<Ctx> = std::sync::OnceLock::new();

/// A disk being served, with the thread that runs its ublk device.
type Served = (Arc<Dev>, Option<std::thread::JoinHandle<()>>);

/// Every disk this worker serves.
static DISKS: Mutex<Vec<Served>> = Mutex::new(Vec::new());

fn disks() -> Vec<Arc<Dev>> {
    DISKS.lock().unwrap().iter().map(|(d, _)| d.clone()).collect()
}

const IO_BUF: u32 = 256 << 10;

/// Fetch workers for reads of absent blocks after a lazy restore.
static FETCH: std::sync::OnceLock<Arc<dev::FetchPool>> = std::sync::OnceLock::new();

fn main() -> Result<()> {
    // glibc gives each thread that allocates its own arena and keeps freed memory there; with
    // segment buffers allocated by one thread and freed by another, RSS settles near 100 MiB more
    // than the live data. Two arenas keep it close to what is actually held.
    #[cfg(target_env = "gnu")]
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, 2);
    }
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("serve") => {
            let path = args.get(2).context("missing CONFIG")?;
            if args.iter().any(|a| a == "--no-supervisor") {
                return worker(path);
            }
            let timeout = args
                .iter()
                .position(|a| a == "--orphan-timeout")
                .and_then(|i| args.get(i + 1))
                .map(|s| s.parse::<u64>())
                .transpose()
                .context("--orphan-timeout takes seconds")?
                .unwrap_or(30);
            load_config(path)?;
            let p = path.clone();
            std::process::exit(supervise::supervise(path, std::time::Duration::from_secs(timeout), move || config_ids(&p))?)
        }
        Some("worker") => worker(args.get(2).context("missing CONFIG")?),
        Some("check-config") => {
            let cfg = load_config(args.get(2).context("missing CONFIG")?)?;
            println!("ok: {} disk(s), store: {}", cfg.devices.len(), if cfg.s3.is_some() { "s3" } else { "local directory" });
            Ok(())
        }
        Some("--version" | "version") => {
            println!("blockgw {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("ctl") => ctl(&args[2], &args[3..].join(" ")),
        Some("del") => {
            // STOP first (a DEL while the server holds the device deadlocks), then delete asynchronously
            let id: i32 = args.get(2).context("missing ID")?.parse()?;
            UblkCtrl::new_simple(id).context("no such ublk device")?;
            supervise::fail_device(id);
            Ok(())
        }
        _ => {
            eprintln!(
                "usage: blockgw serve CONFIG [--orphan-timeout SECS] [--no-supervisor] | check-config CONFIG | ctl SOCKET CMD... | del ID | --version"
            );
            std::process::exit(2)
        }
    }
}

fn load_config(path: &str) -> Result<Config> {
    let mut cfg: Config = serde_json::from_slice(&std::fs::read(path).with_context(|| format!("read {path}"))?).context("parse config")?;
    anyhow::ensure!(cfg.s3.is_some() != cfg.local_store.is_some(), "configure exactly one of \"s3\" and \"local_store\"");
    if let Some(dir) = &cfg.devices_dir {
        for d in dir_disks(dir)? {
            cfg.dynamic.insert(d.id);
            cfg.devices.push(d);
        }
    }
    for (i, d) in cfg.devices.iter().enumerate() {
        validate_dev(d)?;
        check_distinct(d, &cfg.devices[..i])?;
    }
    Ok(cfg)
}

/// The disk files in `devices_dir`, in name order.
fn dir_disks(dir: &str) -> Result<Vec<DevConfig>> {
    std::fs::create_dir_all(dir).with_context(|| format!("devices_dir {dir}"))?;
    let mut files: Vec<_> =
        std::fs::read_dir(dir)?.filter_map(|e| e.ok().map(|e| e.path())).filter(|p| p.extension().is_some_and(|x| x == "json")).collect();
    files.sort();
    files.iter().map(|f| serde_json::from_slice(&std::fs::read(f)?).with_context(|| format!("parse {}", f.display()))).collect()
}

fn validate_dev(d: &DevConfig) -> Result<()> {
    anyhow::ensure!(d.id >= 0, "device id {} must be >= 0", d.id);
    anyhow::ensure!(d.size_gb > 0, "device {}: size_gb must be > 0", d.id);
    anyhow::ensure!(!d.prefix.is_empty() && !d.prefix.ends_with('/'), "device {}: prefix must be non-empty without a trailing '/'", d.id);
    anyhow::ensure!(d.policy == "local" || d.policy == "remote", "device {}: policy must be local or remote", d.id);
    if let Some(k) = &d.key_file {
        crypt::load_key(k).with_context(|| format!("device {}", d.id))?;
    }
    Ok(())
}

/// Two disks may not share an id, an object prefix (they would write one log) or a local dir.
fn check_distinct<'a>(d: &DevConfig, others: impl IntoIterator<Item = &'a DevConfig>) -> Result<()> {
    for o in others {
        anyhow::ensure!(o.id != d.id, "device id {} appears twice", d.id);
        anyhow::ensure!(o.prefix != d.prefix, "devices {} and {} share the prefix {:?}", o.id, d.id, d.prefix);
        anyhow::ensure!(o.dir != d.dir, "devices {} and {} share the dir {:?}", o.id, d.id, d.dir);
    }
    Ok(())
}

/// Ids of every disk the config names, including `devices_dir` (for the supervisor, which re-reads
/// it as disks are attached and detached).
fn config_ids(path: &str) -> Option<Vec<i32>> {
    let cfg: Config = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    let mut ids: Vec<i32> = cfg.devices.iter().map(|d| d.id).collect();
    if let Some(dir) = &cfg.devices_dir {
        ids.extend(dir_disks(dir).ok()?.iter().map(|d| d.id));
    }
    Some(ids)
}

/// The worker: opens every disk, re-uploads what a crashed run left behind, and serves the devices.
fn worker(path: &str) -> Result<()> {
    let cfg = load_config(path)?;
    supervise::install_term_handler();
    let s3: Arc<dyn store::Store> = match (&cfg.s3, &cfg.local_store) {
        (Some(c), None) => Arc::new(s3::S3::new(c)?),
        (None, Some(dir)) => Arc::new(store::LocalDir::new(dir)?),
        _ => unreachable!("validated by load_config"),
    };
    let pool = dev::Pool::new();
    for i in 0..cfg.upload_workers.unwrap_or(16) {
        let p = pool.clone();
        std::thread::Builder::new().name(format!("put-{i}")).spawn(move || p.worker())?;
    }
    let fetch = FETCH.get_or_init(dev::FetchPool::new).clone();
    for i in 0..8 {
        let f = fetch.clone();
        std::thread::Builder::new().name(format!("fetch-{i}")).spawn(move || f.worker())?;
    }
    let _ = CTX.set(Ctx { store: s3, pool, devices_dir: cfg.devices_dir.clone(), dynamic: Mutex::new(cfg.dynamic.clone()) });
    for dc in cfg.devices.clone() {
        start_disk(dc)?;
    }
    // one thread seals every disk's open segment once its oldest write is seal_interval_ms old
    std::thread::Builder::new().name("sealer".into()).spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_millis(100));
        for d in disks() {
            d.seal_if_due();
        }
    })?;
    let _ = std::fs::remove_file(&cfg.control_socket);
    let listener = UnixListener::bind(&cfg.control_socket)?;
    std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            // one thread per request: an attach (a restore) must not hold up stats or drains
            std::thread::spawn(move || {
                let _ = handle_ctl(s);
            });
        }
    });
    if let Some(addr) = &cfg.metrics_addr {
        metrics::serve(addr, move || snapshot(&disks())).with_context(|| format!("metrics listener on {addr}"))?;
        info!("metrics on http://{addr}/metrics");
    }
    // SIGTERM: drain every disk's upload queue, then exit; the devices stay recoverable
    std::thread::Builder::new().name("term".into()).spawn(move || {
        while !supervise::term_requested() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        for d in &disks() {
            let need = d.seal();
            let t0 = std::time::Instant::now();
            while d.committed.load(SeqCst) < need && !d.fenced.load(SeqCst) && t0.elapsed().as_secs() < 60 {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            if let Err(e) = d.checkpoint() {
                warn!("disk {}: checkpoint on shutdown failed: {e:#}", d.cfg.id);
            }
            info!("disk {}: drained to seq {} on shutdown", d.cfg.id, d.committed.load(SeqCst));
        }
        std::process::exit(0);
    })?;
    if cfg.devices_dir.is_some() {
        // disks come and go: keep serving (and accepting attaches) until SIGTERM
        loop {
            std::thread::sleep(std::time::Duration::from_secs(3600));
        }
    }
    // a fixed set of disks: return once every device was deleted
    let handles: Vec<_> = DISKS.lock().unwrap().iter_mut().filter_map(|(_, h)| h.take()).collect();
    for h in handles {
        let _ = h.join();
    }
    Ok(())
}

/// Open a disk, re-upload what a crashed run left behind, and start serving it.
fn start_disk(dc: DevConfig) -> Result<Arc<Dev>> {
    let ctx = CTX.get().expect("worker context");
    failpoint::hit("open")?;
    let store: Arc<dyn store::Store> = match &dc.key_file {
        Some(k) => Arc::new(crypt::CryptStore::new(ctx.store.clone(), &crypt::load_key(k)?)),
        None => ctx.store.clone(),
    };
    let d = Arc::new(Dev::open(dc, store)?);
    let (d2, p2) = (d.clone(), ctx.pool.clone());
    std::thread::Builder::new().name(format!("disp-{}", d.cfg.id)).spawn(move || d2.dispatcher(&p2))?;
    if let Err(e) = d.recover() {
        d.stop();
        return Err(e); // before attach: the crashed run's un-uploaded tail goes first
    }
    let d3 = d.clone();
    std::thread::Builder::new().name(format!("ckpt-{}", d.cfg.id)).spawn(move || d3.checkpointer())?;
    let d4 = d.clone();
    std::thread::Builder::new().name(format!("hydrate-{}", d.cfg.id)).spawn(move || d4.hydrator())?;
    let d5 = d.clone();
    let h = std::thread::Builder::new().name(format!("ublk-{}", d.cfg.id)).spawn(move || {
        if let Err(e) = run_disk(d5) {
            error!("disk thread failed: {e:#}");
        }
    })?;
    DISKS.lock().unwrap().push((d.clone(), Some(h)));
    Ok(d)
}

/// Write a disk file atomically and durably.
fn write_durable(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::File::open(&tmp)?.sync_all()?;
    std::fs::rename(&tmp, path)?;
    std::fs::File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()?;
    Ok(())
}

/// Seal and wait until everything written so far is committed (false: timed out, or fenced).
fn drain_for(d: &Dev, limit: std::time::Duration) -> bool {
    let need = d.seal();
    let t0 = std::time::Instant::now();
    while d.committed.load(SeqCst) < need {
        if d.fenced.load(SeqCst) || t0.elapsed() > limit {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    true
}

/// `ctl attach <disk JSON>`: open a disk at runtime and keep it attached across worker restarts.
fn attach(json: &str) -> Result<String> {
    let ctx = CTX.get().expect("worker context");
    let dir = ctx.devices_dir.as_ref().context("attach needs \"devices_dir\" in the config")?;
    let dc: DevConfig = serde_json::from_str(json).context("parse the disk JSON")?;
    validate_dev(&dc)?;
    let current: Vec<DevConfig> = disks().iter().map(|d| d.cfg.clone()).collect();
    check_distinct(&dc, &current)?;
    let id = dc.id;
    anyhow::ensure!(supervise::device_state(id).is_none(), "ublk device {id} already exists (another server, or not deleted yet)");
    let path = Path::new(dir).join(format!("{id}.json"));
    write_durable(&path, json.as_bytes())?;
    ctx.dynamic.lock().unwrap().insert(id);
    let fail = |e: anyhow::Error| {
        ctx.dynamic.lock().unwrap().remove(&id);
        let _ = std::fs::remove_file(&path);
        e
    };
    let d = start_disk(dc).map_err(fail)?;
    let t0 = std::time::Instant::now();
    loop {
        if supervise::device_state(id) == Some(libublk::sys::UBLK_S_DEV_LIVE) {
            return Ok(format!("ok disk {id} attached as /dev/ublkb{id} in {} ms", t0.elapsed().as_millis()));
        }
        let finished = DISKS.lock().unwrap().iter().any(|(x, h)| Arc::ptr_eq(x, &d) && h.as_ref().is_none_or(|h| h.is_finished()));
        if finished {
            d.stop();
            DISKS.lock().unwrap().retain(|(x, _)| !Arc::ptr_eq(x, &d));
            return Err(fail(anyhow::anyhow!("disk {id}: the device did not start (see the log)")));
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// Is the block device claimed (mounted, or held open with O_EXCL), in any mount namespace? An
/// exclusive open fails with EBUSY then. The open goes through a private device node made from
/// the number in sysfs, not /dev/ublkbN, which can be missing or stale (containers without udev).
/// An open that fails for any other reason is an error: the check must not pass by accident.
fn in_use(n: i32, dir: &str) -> Result<bool> {
    use std::os::unix::fs::OpenOptionsExt;
    let num = std::fs::read_to_string(format!("/sys/block/ublkb{n}/dev")).with_context(|| format!("disk {n}: no block device in sysfs"))?;
    let (maj, min) = num.trim().split_once(':').context("sysfs dev number")?;
    let node = Path::new(dir).join(".excl-check");
    let _ = std::fs::remove_file(&node);
    let c = std::ffi::CString::new(node.as_os_str().as_encoded_bytes())?;
    let dev = libc::makedev(maj.parse()?, min.parse()?);
    if unsafe { libc::mknod(c.as_ptr(), libc::S_IFBLK | 0o600, dev) } != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| format!("disk {n}: create a device node to check it is unused"));
    }
    let r = std::fs::OpenOptions::new().read(true).custom_flags(libc::O_EXCL).open(&node);
    let _ = std::fs::remove_file(&node);
    match r {
        Ok(_) => Ok(false),
        Err(e) if e.raw_os_error() == Some(libc::EBUSY) => Ok(true),
        Err(e) => Err(e).with_context(|| format!("disk {n}: check that the device is unused")),
    }
}

/// `ctl detach <id> [purge]`: stop serving a disk attached with `attach`, after its writes are in
/// the object store. `purge` also removes the local image: the disk is then only in the object
/// store, and a later attach restores it (lazily).
fn detach(id: &str, purge: bool) -> Result<String> {
    let ctx = CTX.get().expect("worker context");
    let n: i32 = id.parse().context("disk id")?;
    let d = disks().into_iter().find(|d| d.cfg.id == n).context("no such disk")?;
    anyhow::ensure!(
        ctx.dynamic.lock().unwrap().contains(&n),
        "disk {n} comes from the config file's \"devices\"; only disks attached with `attach` can be detached"
    );
    anyhow::ensure!(!in_use(n, &d.cfg.dir)?, "disk {n} is in use (mounted or held open); unmount it first");
    let limit = std::time::Duration::from_secs(60);
    anyhow::ensure!(drain_for(&d, limit), "disk {n}: uploads did not drain within 60 s (or the disk is fenced); it stays attached");
    // stop the device: nothing can write any more; then commit whatever slipped in meanwhile
    UblkCtrl::new_simple(n)?.kill_dev()?;
    anyhow::ensure!(
        drain_for(&d, limit),
        "disk {n}: stopped, but the last uploads did not drain; the next worker start recovers it from its local image"
    );
    if let Err(e) = d.checkpoint() {
        warn!("disk {n}: checkpoint before detach failed: {e:#}");
    }
    d.stop();
    supervise::remove_device(n);
    let h = DISKS.lock().unwrap().iter_mut().find(|(x, _)| Arc::ptr_eq(x, &d)).and_then(|(_, h)| h.take());
    if let Some(h) = h {
        let _ = h.join();
    }
    DISKS.lock().unwrap().retain(|(x, _)| !Arc::ptr_eq(x, &d));
    ctx.dynamic.lock().unwrap().remove(&n);
    if let Some(dir) = &ctx.devices_dir {
        let _ = std::fs::remove_file(Path::new(dir).join(format!("{n}.json")));
        std::fs::File::open(dir)?.sync_all()?;
    }
    if purge {
        std::fs::remove_dir_all(&d.cfg.dir).with_context(|| format!("remove {}", d.cfg.dir))?;
    }
    info!("disk {n}: detached{}", if purge { ", local data removed" } else { "" });
    Ok(format!("ok disk {n} detached{}", if purge { ", local data removed" } else { "" }))
}

fn run_disk(d: Arc<Dev>) -> Result<()> {
    let id = d.cfg.id;
    let recover = matches!(supervise::attach_mode(id)?, supervise::Attach::Recover);
    if recover {
        info!("disk {id}: re-attaching the existing device");
        UblkCtrl::new_simple(id)?.start_user_recover()?;
    }
    let ctrl = libublk::ctrl::UblkCtrlBuilder::default()
        .name("blockgw")
        .id(id)
        .nr_queues(d.cfg.queues)
        .depth(d.cfg.depth)
        .io_buf_bytes(IO_BUF)
        .ctrl_flags((libublk::sys::UBLK_F_USER_RECOVERY | libublk::sys::UBLK_F_USER_RECOVERY_REISSUE) as u64)
        .dev_flags(if recover { UblkFlags::UBLK_DEV_F_RECOVER_DEV } else { UblkFlags::UBLK_DEV_F_ADD_DEV })
        .build()?;
    let size = d.size;
    let fd = d.image.as_raw_fd();
    let tgt_init = move |dev: &mut UblkDev| {
        let t = &mut dev.tgt;
        t.fds[t.nr_fds as usize] = fd;
        t.nr_fds += 1;
        dev.set_default_params(size);
        // 4 KiB logical blocks: every I/O is block aligned, which the extent map relies on
        dev.tgt.params.basic.logical_bs_shift = 12;
        dev.tgt.params.basic.physical_bs_shift = 12;
        dev.tgt.params.basic.io_min_shift = 12;
        let p = &mut dev.tgt.params;
        p.types |= libublk::sys::UBLK_PARAM_TYPE_DISCARD;
        p.discard.discard_granularity = 4096;
        p.discard.max_discard_sectors = (64 << 20) >> 9;
        p.discard.max_write_zeroes_sectors = (64 << 20) >> 9;
        p.discard.max_discard_segments = 1;
        Ok(())
    };
    let dq = d.clone();
    ctrl.run_target(
        tgt_init,
        move |qid, dev: &_| queue_fn(qid, dev, dq.clone()),
        move |c: &UblkCtrl| {
            info!("disk {id}: /dev/ublkb{} ready", c.dev_info().dev_id);
        },
    )?;
    Ok(())
}

fn queue_fn(qid: u16, dev: &UblkDev, d: Arc<Dev>) {
    let q = Rc::new(UblkQueue::new(qid, dev).unwrap());
    let exe = Rc::new(smol::LocalExecutor::new());
    let mut tasks = Vec::new();
    for tag in q.tags() {
        let (q, d) = (q.clone(), d.clone());
        tasks.push(exe.spawn(async move {
            match io_task(&q, tag, &d).await {
                Err(UblkError::QueueIsDown) | Ok(_) => {}
                Err(e) => error!("io task {tag}: {e}"),
            }
        }));
    }
    let e2 = exe.clone();
    smol::block_on(exe.run(async move {
        let run = || while e2.try_tick() {};
        let done = || tasks.iter().all(|t| t.is_finished());
        if let Err(e) = libublk::wait_and_handle_io_events(&q, Some(20), run, done).await {
            error!("queue loop: {e}");
        }
    }));
}

async fn io_task(q: &UblkQueue<'_>, tag: u16, d: &Arc<Dev>) -> Result<(), UblkError> {
    let mut buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);
    q.submit_io_prep_cmd(tag, BufDesc::Slice(buf.as_slice()), 0, Some(&buf)).await?;
    loop {
        let res = handle(q, tag, d, buf.as_mut_slice()).await;
        q.submit_io_commit_cmd(tag, BufDesc::Slice(buf.as_slice()), res).await?;
    }
}

async fn uring(sqe: io_uring::squeue::Entry) -> i32 {
    ublk_submit_sqe_async(sqe, libublk::UblkUringData::Target as u64).await.unwrap_or(-libc::EIO)
}

/// Wait on a fresh eventfd through the queue's own io_uring, so the queue keeps serving other tags.
async fn wait_efd(register: impl FnOnce(i32)) {
    let efd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    register(efd);
    let mut b = [0u8; 8];
    let _ = uring(opcode::Read::new(types::Fd(efd), b.as_mut_ptr(), 8).build()).await;
    unsafe { libc::close(efd) };
}

async fn flush(d: &Dev) -> i32 {
    let presence = d.presence_snapshot(); // before the sync: see Dev::presence_snapshot
    let r = uring(opcode::Fsync::new(types::Fixed(1)).flags(types::FsyncFlags::DATASYNC).build()).await;
    if r < 0 {
        return r;
    }
    if let Some(pages) = presence {
        if let Err(e) = d.presence_persist(pages) {
            warn!("disk {}: persisting the presence bitmap failed: {e:#}", d.cfg.id);
            return -libc::EIO;
        }
    }
    d.stats.flushes.fetch_add(1, Relaxed);
    let need = if d.flush_seals() { d.seal() } else { 0 };
    let t0 = std::time::Instant::now();
    if d.policy.load(SeqCst) == POLICY_REMOTE {
        wait_efd(|fd| d.wait_committed(need, fd)).await;
    } else if d.queued_bytes() > d.max_lag.load(SeqCst) {
        d.stats.lag_waits.fetch_add(1, Relaxed);
        wait_efd(|fd| d.wait_lag_below(d.max_lag.load(SeqCst), fd)).await;
    }
    d.stats.flush_wait_micros.fetch_add(t0.elapsed().as_micros() as u64, Relaxed);
    if d.fenced.load(SeqCst) {
        -libc::EIO
    } else {
        0
    }
}

/// Serve one request; every failure (other than a fenced disk, which logs FENCED once) is logged
/// with its cause, since the kernel only reports "I/O error" whatever the errno.
async fn handle(q: &UblkQueue<'_>, tag: u16, d: &Arc<Dev>, io: &mut [u8]) -> i32 {
    let res = handle_op(q, tag, d, io).await;
    if res < 0 && !d.fenced.load(SeqCst) {
        let iod = q.get_iod(tag);
        warn!(
            "disk {}: I/O failed: op {} at byte {} len {}: {}",
            d.cfg.id,
            iod.op_flags & 0xff,
            iod.start_sector << 9,
            iod.nr_sectors << 9,
            std::io::Error::from_raw_os_error(-res)
        );
    }
    res
}

async fn handle_op(_q: &UblkQueue<'_>, tag: u16, d: &Arc<Dev>, io: &mut [u8]) -> i32 {
    if d.fenced.load(SeqCst) {
        return -libc::EIO;
    }
    let iod = _q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let bytes = iod.nr_sectors << 9;
    match op {
        libublk::sys::UBLK_IO_OP_READ => {
            if d.has_absent(off, bytes as u64) {
                // lazy restore: fetch the missing blocks off the queue thread, then read the image
                let failed = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let (dd, f2) = (d.clone(), failed.clone());
                wait_efd(|fd| FETCH.get().expect("fetch pool").submit(dd, off, bytes as u64, fd, f2)).await;
                if failed.load(SeqCst) != 0 {
                    return -libc::EIO;
                }
            }
            uring(opcode::Read::new(types::Fixed(1), io.as_mut_ptr(), bytes).offset(off).build()).await
        }
        libublk::sys::UBLK_IO_OP_WRITE => {
            let rg = match d.intent_begin(off, bytes as u64) {
                Ok(rg) => rg,
                Err(e) => {
                    warn!("disk {}: persisting the write-intent map failed: {e:#}", d.cfg.id);
                    return -libc::EIO;
                }
            };
            d.write_begin(off, bytes as u64);
            let r = uring(opcode::Write::new(types::Fixed(1), io.as_ptr(), bytes).offset(off).build()).await;
            d.write_end(off, bytes as u64, r >= 0);
            let seq = (r >= 0).then(|| d.append_data(off, &io[..bytes as usize]));
            d.intent_end(rg, seq);
            // bulk writes without FLUSH must not grow the upload queue without bound
            let hard = (4 * d.max_lag.load(SeqCst)).max(64 << 20);
            if r >= 0 && d.queued_bytes() > hard {
                d.stats.lag_waits.fetch_add(1, Relaxed);
                wait_efd(|fd| d.wait_lag_below(hard, fd)).await;
            }
            if r >= 0 && iod.op_flags & libublk::sys::UBLK_IO_F_FUA != 0 {
                let f = flush(d).await;
                if f < 0 {
                    return f;
                }
            }
            r
        }
        libublk::sys::UBLK_IO_OP_FLUSH => flush(d).await,
        libublk::sys::UBLK_IO_OP_DISCARD | libublk::sys::UBLK_IO_OP_WRITE_ZEROES => {
            let rg = match d.intent_begin(off, bytes as u64) {
                Ok(rg) => rg,
                Err(e) => {
                    warn!("disk {}: persisting the write-intent map failed: {e:#}", d.cfg.id);
                    return -libc::EIO;
                }
            };
            d.write_begin(off, bytes as u64);
            let (r, seq) = match log::punch(d.image.as_raw_fd(), off, bytes as u64).and_then(|_| d.zero_absent(off, bytes as u64)) {
                Ok(_) => (0, Some(d.append_discard(off, bytes))),
                Err(e) => {
                    warn!("disk {}: discard in the image failed: {e:#}", d.cfg.id);
                    (-libc::EIO, None)
                }
            };
            d.write_end(off, bytes as u64, r == 0);
            d.intent_end(rg, seq);
            r
        }
        _ => -libc::EINVAL,
    }
}

fn snapshot(devs: &[Arc<Dev>]) -> metrics::Snapshot {
    metrics::Snapshot { version: env!("CARGO_PKG_VERSION"), rss_bytes: rss_kb() * 1024, disks: devs.iter().map(|d| d.snapshot()).collect() }
}

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)?.parse().ok()))
        .unwrap_or(0)
}

fn handle_ctl(s: UnixStream) -> Result<()> {
    let mut w = s.try_clone()?;
    let mut line = String::new();
    BufReader::new(s).read_line(&mut line)?;
    let a: Vec<&str> = line.split_whitespace().collect();
    let devs = disks();
    let devs = &devs[..];
    let find = |id: &str| devs.iter().find(|d| d.cfg.id.to_string() == id).cloned();
    let out = match a.as_slice() {
        ["attach", ..] => attach(line.trim_start().trim_start_matches("attach").trim()).unwrap_or_else(|e| format!("error: {e:#}")),
        ["detach", id] => detach(id, false).unwrap_or_else(|e| format!("error: {e:#}")),
        ["detach", id, "purge"] => detach(id, true).unwrap_or_else(|e| format!("error: {e:#}")),
        ["policy", id, p] => match find(id) {
            Some(d) => {
                d.policy.store(if *p == "remote" { POLICY_REMOTE } else { POLICY_LOCAL }, SeqCst);
                format!("ok disk {id} policy={p}")
            }
            None => "error: no such disk".into(),
        },
        ["lag", id, mb] => match find(id) {
            Some(d) => {
                d.max_lag.store(mb.parse::<u64>().unwrap_or(16) << 20, SeqCst);
                format!("ok disk {id} max_lag={mb}MiB")
            }
            None => "error: no such disk".into(),
        },
        ["drain", id] => match find(id) {
            Some(d) => {
                let need = d.seal();
                let t0 = std::time::Instant::now();
                while d.committed.load(SeqCst) < need && !d.fenced.load(SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                let ck = match d.checkpoint() {
                    Ok(true) => "checkpointed",
                    Ok(false) => "checkpoint current",
                    Err(_) => "checkpoint failed",
                };
                format!("ok disk {id} drained to seq {need} in {} ms, {ck}", t0.elapsed().as_millis())
            }
            None => "error: no such disk".into(),
        },
        ["stats"] => {
            let s = snapshot(devs);
            let mut v = vec![format!("rss_kb {}", s.rss_bytes / 1024)];
            for d in &s.disks {
                v.push(format!(
                    "disk {} epoch={} policy={} fenced={} committed_seq={} queued_bytes={} open_bytes={} puts={} put_bytes={} raw_bytes={} upload_errors={} flushes={} intent_syncs={} lag_waits={}",
                    d.id, d.epoch, d.policy, d.fenced, d.committed_seq, d.queued_bytes, d.open_bytes, d.puts, d.put_bytes, d.raw_bytes,
                    d.upload_errors, d.flushes, d.intent_syncs, d.lag_waits
                ));
            }
            v.join("\n")
        }
        ["stats", "json"] => serde_json::to_string(&snapshot(devs))?,
        ["gc", id] => match find(id) {
            Some(d) => {
                // drain and checkpoint first, so the round sees everything committed
                let need = d.seal();
                while d.committed.load(SeqCst) < need && !d.fenced.load(SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                match d.checkpoint().and_then(|_| d.gc_round()) {
                    Ok((n, b)) => format!("ok disk {id} gc deleted {n} segments, repacked {b} bytes"),
                    Err(e) => format!("error: {e:#}"),
                }
            }
            None => "error: no such disk".into(),
        },
        ["metrics"] => metrics::render_prometheus(&snapshot(devs)).trim_end().to_string(),
        _ => "error: unknown command".into(),
    };
    writeln!(w, "{out}")?;
    Ok(())
}

fn ctl(sock: &str, cmd: &str) -> Result<()> {
    let mut s = UnixStream::connect(sock)?;
    writeln!(s, "{cmd}")?;
    let mut out = String::new();
    std::io::Read::read_to_string(&mut s, &mut out)?;
    print!("{out}");
    Ok(())
}
