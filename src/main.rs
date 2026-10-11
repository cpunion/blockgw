//! blockgw: one process serves every virtual disk on this host through ublk; each disk lives in a
//! local sparse image and persists as a log of segments in S3-compatible object storage.
//!
//!   blockgw serve CONFIG.json [--orphan-timeout SECS] [--no-supervisor]
//!       serve every configured disk; devices left by a crashed server are re-attached, others added
//!   blockgw ctl SOCKET <command...>          policy <id> local|remote | lag <id> <MiB> | drain <id> | stats
//!   blockgw del <ublk-id>                    stop and delete a ublk device
//!
//! Data path per I/O (queue thread, io_uring shared with the ublk queue):
//!   READ  -> read from the local sparse data file (O_DIRECT)
//!   WRITE -> intent bit durable (first touch of a 4 MiB region only) -> write data file -> copy into open segment
//!   FLUSH -> fdatasync data file -> seal open segment -> policy wait (remote: uploaded; local: lag <= max_lag)
//!   DISCARD / WRITE_ZEROES -> punch hole -> record in open segment

mod dev;
mod failpoint;
mod log;
mod logx;
mod metrics;
mod s3;
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
use std::io::{BufRead, BufReader, Write};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::rc::Rc;
use std::sync::atomic::Ordering::*;
use std::sync::Arc;

#[derive(serde::Deserialize)]
struct Config {
    /// S3-compatible object store for the segment log...
    s3: Option<s3::S3Config>,
    /// ...or a local directory (development, tests, or a second disk / NAS mount)
    local_store: Option<String>,
    /// optional "host:port" for a Prometheus `GET /metrics` endpoint
    metrics_addr: Option<String>,
    control_socket: String,
    devices: Vec<DevConfig>,
    upload_workers: Option<usize>,
}

const IO_BUF: u32 = 256 << 10;

fn main() -> Result<()> {
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
            let cfg = load_config(path)?;
            let ids: Vec<i32> = cfg.devices.iter().map(|d| d.id).collect();
            std::process::exit(supervise::supervise(path, &ids, std::time::Duration::from_secs(timeout))?)
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
    let cfg: Config = serde_json::from_slice(&std::fs::read(path).with_context(|| format!("read {path}"))?).context("parse config")?;
    anyhow::ensure!(cfg.s3.is_some() != cfg.local_store.is_some(), "configure exactly one of \"s3\" and \"local_store\"");
    let mut ids = std::collections::HashSet::new();
    for d in &cfg.devices {
        anyhow::ensure!(ids.insert(d.id), "device id {} appears twice", d.id);
        anyhow::ensure!(d.id >= 0, "device id {} must be >= 0", d.id);
        anyhow::ensure!(d.size_gb > 0, "device {}: size_gb must be > 0", d.id);
        anyhow::ensure!(
            !d.prefix.is_empty() && !d.prefix.ends_with('/'),
            "device {}: prefix must be non-empty without a trailing '/'",
            d.id
        );
        anyhow::ensure!(d.policy == "local" || d.policy == "remote", "device {}: policy must be local or remote", d.id);
    }
    Ok(cfg)
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
        let (p, s) = (pool.clone(), s3.clone());
        std::thread::Builder::new().name(format!("put-{i}")).spawn(move || p.worker(s.as_ref()))?;
    }
    let mut devs = Vec::new();
    for dc in cfg.devices.clone() {
        failpoint::hit("open")?;
        let d = Arc::new(Dev::open(dc, s3.as_ref())?);
        let (d2, p2) = (d.clone(), pool.clone());
        std::thread::Builder::new().name(format!("disp-{}", d.cfg.id)).spawn(move || d2.dispatcher(&p2))?;
        d.recover()?; // before attach: the crashed run's un-uploaded tail goes first
        devs.push(d);
    }
    let ctl_devs = devs.clone();
    let _ = std::fs::remove_file(&cfg.control_socket);
    let listener = UnixListener::bind(&cfg.control_socket)?;
    std::thread::spawn(move || {
        for s in listener.incoming().flatten() {
            let _ = handle_ctl(s, &ctl_devs);
        }
    });
    if let Some(addr) = &cfg.metrics_addr {
        let mdevs = devs.clone();
        metrics::serve(addr, move || snapshot(&mdevs)).with_context(|| format!("metrics listener on {addr}"))?;
        info!("metrics on http://{addr}/metrics");
    }
    // SIGTERM: drain every disk's upload queue, then exit; the devices stay recoverable
    let term_devs = devs.clone();
    std::thread::Builder::new().name("term".into()).spawn(move || {
        while !supervise::term_requested() {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        for d in &term_devs {
            let need = d.seal();
            let t0 = std::time::Instant::now();
            while d.committed.load(SeqCst) < need && !d.fenced.load(SeqCst) && t0.elapsed().as_secs() < 60 {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            info!("disk {}: drained to seq {} on shutdown", d.cfg.id, d.committed.load(SeqCst));
        }
        std::process::exit(0);
    })?;
    let mut handles = Vec::new();
    for d in devs {
        handles.push(std::thread::Builder::new().name(format!("ublk-{}", d.cfg.id)).spawn(move || run_disk(d))?);
    }
    for h in handles {
        if let Ok(Err(e)) = h.join() {
            error!("disk thread failed: {e:#}");
        }
    }
    Ok(())
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

async fn io_task(q: &UblkQueue<'_>, tag: u16, d: &Dev) -> Result<(), UblkError> {
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
    let r = uring(opcode::Fsync::new(types::Fixed(1)).flags(types::FsyncFlags::DATASYNC).build()).await;
    if r < 0 {
        return r;
    }
    d.stats.flushes.fetch_add(1, Relaxed);
    let need = d.seal();
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

async fn handle(_q: &UblkQueue<'_>, tag: u16, d: &Dev, io: &mut [u8]) -> i32 {
    if d.fenced.load(SeqCst) {
        return -libc::EIO;
    }
    let iod = _q.get_iod(tag);
    let op = iod.op_flags & 0xff;
    let off = iod.start_sector << 9;
    let bytes = iod.nr_sectors << 9;
    match op {
        libublk::sys::UBLK_IO_OP_READ => uring(opcode::Read::new(types::Fixed(1), io.as_mut_ptr(), bytes).offset(off).build()).await,
        libublk::sys::UBLK_IO_OP_WRITE => {
            let Ok(rg) = d.intent_begin(off, bytes as u64) else { return -libc::EIO };
            let r = uring(opcode::Write::new(types::Fixed(1), io.as_ptr(), bytes).offset(off).build()).await;
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
            let Ok(rg) = d.intent_begin(off, bytes as u64) else { return -libc::EIO };
            let (r, seq) = match log::punch(d.image.as_raw_fd(), off, bytes as u64) {
                Ok(_) => (0, Some(d.append_discard(off, bytes))),
                Err(_) => (-libc::EIO, None),
            };
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

fn handle_ctl(s: UnixStream, devs: &[Arc<Dev>]) -> Result<()> {
    let mut w = s.try_clone()?;
    let mut line = String::new();
    BufReader::new(s).read_line(&mut line)?;
    let a: Vec<&str> = line.split_whitespace().collect();
    let find = |id: &str| devs.iter().find(|d| d.cfg.id.to_string() == id).cloned();
    let out = match a.as_slice() {
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
                format!("ok disk {id} drained to seq {need} in {} ms", t0.elapsed().as_millis())
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
