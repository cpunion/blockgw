//! Process supervision and device lifecycle.
//!
//! `blockgw serve` runs a small supervisor that starts the worker (`blockgw worker CONFIG`) and
//! watches the configured ublk devices:
//! * if the worker exits abnormally, it is restarted at once; the new worker re-attaches the devices
//!   (ublk user recovery), so consumers see a stall, not an error;
//! * if the devices stay without a live server for longer than the orphan timeout (the worker keeps
//!   crashing, hangs, or cannot reach the object store), the supervisor stops and deletes them.
//!   Queued and future I/O then fails with EIO instead of hanging forever, and the supervisor exits
//!   with status 1 so an outer service manager can decide what to do next;
//! * SIGTERM/SIGINT are forwarded to the worker, which drains its upload queues and exits, leaving
//!   the devices recoverable by the next start.

use anyhow::{bail, Result};
use libublk::ctrl::UblkCtrl;
use libublk::sys::{UBLK_S_DEV_DEAD, UBLK_S_DEV_FAIL_IO, UBLK_S_DEV_LIVE, UBLK_S_DEV_QUIESCED};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::{Duration, Instant};

static TERM: AtomicBool = AtomicBool::new(false);

extern "C" fn on_term(_: libc::c_int) {
    TERM.store(true, SeqCst);
}

pub fn install_term_handler() {
    unsafe {
        libc::signal(libc::SIGTERM, on_term as *const () as libc::sighandler_t);
        libc::signal(libc::SIGINT, on_term as *const () as libc::sighandler_t);
    }
}

pub fn term_requested() -> bool {
    TERM.load(SeqCst)
}

/// Kernel state of ublk device `id`, `None` if it does not exist.
pub fn device_state(id: i32) -> Option<u32> {
    UblkCtrl::new_simple(id).ok().map(|c| c.dev_info().state as u32)
}

pub enum Attach {
    Add,
    Recover,
}

/// Decide how a starting worker attaches device `id`: recover a device whose server is gone, add a
/// missing one, and refuse to touch one that another server is serving.
pub fn attach_mode(id: i32) -> Result<Attach> {
    for _ in 0..50 {
        match device_state(id) {
            None => return Ok(Attach::Add),
            Some(s) if s == UBLK_S_DEV_QUIESCED || s == UBLK_S_DEV_FAIL_IO => return Ok(Attach::Recover),
            Some(s) if s == UBLK_S_DEV_DEAD => {
                // stopped, or created but never started: delete it and add a fresh one
                remove_device(id);
                for _ in 0..50 {
                    if device_state(id).is_none() {
                        return Ok(Attach::Add);
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                bail!("ublk device {id} is stopped but still in use (unmount the filesystem on /dev/ublkb{id})");
            }
            Some(_) => std::thread::sleep(Duration::from_millis(100)), // live: the old server may still be exiting
        }
    }
    bail!("ublk device {id} is live: another server is serving it")
}

/// Stop a device so that pending and future I/O fails instead of waiting for a server, then delete
/// it. The delete is asynchronous: while a filesystem is still mounted on the device, the kernel
/// keeps the device until the last reference goes away, and a synchronous delete would block.
pub fn fail_device(id: i32) {
    if let Ok(c) = UblkCtrl::new_simple(id) {
        let _ = c.kill_dev();
    }
    remove_device(id);
}

/// Asynchronous delete (STOP must already have happened, or the device must have no server).
pub fn remove_device(id: i32) {
    if let Ok(c) = UblkCtrl::new_simple(id) {
        let _ = c.del_dev_async();
    }
}

/// Run the worker under supervision until it exits cleanly, a termination signal arrives, or the
/// devices stay orphaned for longer than `orphan_timeout`.
///
/// `ids` lists the configured disks (re-read periodically: disks can be attached at runtime). Only
/// devices that exist count: a device that is not live has I/O waiting for a server, while a disk
/// still being attached has no device yet.
pub fn supervise(config: &str, orphan_timeout: Duration, ids: impl Fn() -> Option<Vec<i32>>) -> Result<i32> {
    install_term_handler();
    let exe = std::env::current_exe()?;
    let mut backoff = Duration::from_millis(200);
    let mut down_since: Option<Instant> = Some(Instant::now()); // nothing is live before the first start
    let mut known = ids().unwrap_or_default();
    let mut read_at = Instant::now();
    loop {
        let mut child = Command::new(&exe).arg("worker").arg(config).spawn()?;
        let started = Instant::now();
        let status = loop {
            if term_requested() {
                unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
                let st = child.wait()?;
                return Ok(st.code().unwrap_or(1));
            }
            if let Some(st) = child.try_wait()? {
                break st;
            }
            if read_at.elapsed() > Duration::from_secs(1) {
                known = ids().unwrap_or(known);
                read_at = Instant::now();
            }
            let all_live = known.iter().filter_map(|id| device_state(*id)).all(|s| s == UBLK_S_DEV_LIVE);
            if all_live {
                down_since = None;
                backoff = Duration::from_millis(200);
            } else if let Some(t) = down_since {
                if t.elapsed() > orphan_timeout {
                    crate::warn!("supervisor: devices without a live server for {:?}; stopping the worker", t.elapsed());
                    let _ = child.kill();
                    let _ = child.wait();
                    break std::process::ExitStatus::default(); // handled below as an orphan timeout
                }
            } else {
                down_since = Some(Instant::now());
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        if status.success() && down_since.is_none_or(|t| t.elapsed() <= orphan_timeout) {
            return Ok(0); // the worker returned on its own: every device was deleted
        }
        let since = *down_since.get_or_insert_with(Instant::now);
        if since.elapsed() > orphan_timeout {
            let known = ids().unwrap_or(known);
            crate::warn!("supervisor: orphan timeout ({orphan_timeout:?}) exceeded; failing devices {known:?}");
            for id in &known {
                fail_device(*id);
            }
            return Ok(1);
        }
        crate::warn!("supervisor: worker exited ({status}) after {:?}; restarting in {backoff:?}", started.elapsed());
        std::thread::sleep(backoff);
        backoff = (backoff * 2).min(Duration::from_secs(2));
    }
}
