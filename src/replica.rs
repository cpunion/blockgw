//! Standby replication: a second host keeps the writes that are acknowledged but not yet in the
//! object store, so that losing the primary host loses no acknowledged write under `local`.
//!
//! The standby (`blockgw replica --listen ADDR --dir DIR`) is a dumb journal: per disk prefix it
//! stores what the primary sends, fsyncs, and acknowledges. It never talks to the object store and
//! needs no key: when the disk is encrypted, every batch is sealed with the disk key first.
//!
//! The primary sends data at two points only, both under the open-segment lock, so frames follow
//! the order of the log:
//! * at a flush (fsync), the part of the open segment not sent yet, then a barrier; the flush
//!   returns once the standby has acknowledged the barrier (the data is on its disk);
//! * when a segment is sealed, the rest of it and a SEAL marker. A sealed segment is handed to the
//!   uploader only after the standby has acknowledged its SEAL, so whenever segment N+1 is in the
//!   object store, the standby holds all of N.
//!
//! If the standby is unreachable, the primary degrades: flushes wait for the object store instead
//! (as under `remote`), and uploads no longer wait for the standby. On reconnecting it sends SYNC,
//! which drops every segment without a SEAL marker from the journal (their content may have holes),
//! and then the open segment in full.
//!
//! Taking a disk over (`fetch`, before the new writer claims its epoch): every sealed segment the
//! store lacks is rebuilt from the journal and uploaded create-only; an unsealed last segment is
//! uploaded too if the store holds nothing after it (it ends with the last acknowledged flush).
//! Then the journal of those epochs is dropped.
//!
//! Wire format: frames of `u32 length | u8 type | payload`, integers little-endian.

use anyhow::{bail, ensure, Context, Result};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

const HELLO: u8 = 1;
const APPEND: u8 = 2;
const SEAL: u8 = 3;
const SYNC: u8 = 4;
const COMMITTED: u8 = 5;
const BARRIER: u8 = 6;
const ACK: u8 = 7;
const FETCH: u8 = 8;
const ITEM: u8 = 9;
const END: u8 = 10;
const DROP: u8 = 11;
const REFUSED: u8 = 12;

const ACK_TIMEOUT: Duration = Duration::from_secs(3);

fn put_u64(v: &mut Vec<u8>, x: u64) {
    v.extend_from_slice(&x.to_le_bytes());
}
fn put_str(v: &mut Vec<u8>, s: &str) {
    v.extend_from_slice(&(s.len() as u16).to_le_bytes());
    v.extend_from_slice(s.as_bytes());
}

struct Cursor<'a>(&'a [u8]);
impl Cursor<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8]> {
        ensure!(self.0.len() >= n, "short frame");
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    fn str(&mut self) -> Result<String> {
        let n = u16::from_le_bytes(self.take(2)?.try_into()?) as usize;
        Ok(String::from_utf8(self.take(n)?.to_vec())?)
    }
    fn rest(&mut self) -> &[u8] {
        std::mem::take(&mut self.0)
    }
}

fn write_frame(w: &mut impl Write, t: u8, payload: &[u8]) -> std::io::Result<()> {
    w.write_all(&(payload.len() as u32 + 1).to_le_bytes())?;
    w.write_all(&[t])?;
    w.write_all(payload)
}

fn read_frame(r: &mut impl Read) -> std::io::Result<(u8, Vec<u8>)> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len == 0 || len > 64 << 20 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad frame length"));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    let t = buf.remove(0);
    Ok((t, buf))
}

// ---------------------------------------------------------------------------------------------
// The standby

/// Journal layout: `DIR/<hex prefix>/<epoch:010>/<seq:012>.rec` (a sequence of `n u32 | len u32 |
/// payload` batches), `<seq:012>.sealed` once sealed, and `DIR/<hex prefix>/epoch` holding the
/// highest epoch dropped below, so a fenced primary cannot write its old epoch back.
pub fn serve(listen: &str, dir: &str) -> Result<()> {
    fs::create_dir_all(dir)?;
    let listener = TcpListener::bind(listen).with_context(|| format!("listen on {listen}"))?;
    crate::info!("replica: listening on {listen}, journal in {dir}");
    let store = Arc::new(Journal { root: PathBuf::from(dir), lock: Mutex::new(()) });
    for s in listener.incoming().flatten() {
        let j = store.clone();
        std::thread::spawn(move || {
            let peer = s.peer_addr().map(|a| a.to_string()).unwrap_or_default();
            if let Err(e) = j.session(s) {
                crate::warn!("replica: session with {peer} ended: {e:#}");
            }
        });
    }
    Ok(())
}

struct Journal {
    root: PathBuf,
    lock: Mutex<()>, // journal operations are short; one at a time keeps them simple
}

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

impl Journal {
    fn disk(&self, prefix: &str) -> PathBuf {
        self.root.join(hex(prefix))
    }
    fn epoch_dir(&self, prefix: &str, epoch: u64) -> PathBuf {
        self.disk(prefix).join(format!("{epoch:010}"))
    }
    fn floor(&self, prefix: &str) -> u64 {
        fs::read_to_string(self.disk(prefix).join("epoch")).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0)
    }

    fn session(&self, s: TcpStream) -> Result<()> {
        s.set_nodelay(true)?;
        let mut r = BufReader::new(s.try_clone()?);
        let mut w = BufWriter::new(s);
        let mut cur: Option<(String, u64)> = None;
        let mut touched: HashSet<PathBuf> = HashSet::new();
        loop {
            let (t, payload) = match read_frame(&mut r) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e.into()),
            };
            let mut c = Cursor(&payload);
            let _g = self.lock.lock().unwrap();
            match t {
                HELLO => {
                    let (prefix, epoch) = (c.str()?, c.u64()?);
                    if epoch < self.floor(&prefix) {
                        write_frame(&mut w, REFUSED, b"epoch already taken over")?;
                        w.flush()?;
                        bail!("refused {prefix} epoch {epoch}: a newer writer took the disk over");
                    }
                    fs::create_dir_all(self.epoch_dir(&prefix, epoch))?;
                    cur = Some((prefix, epoch));
                }
                APPEND | SEAL | SYNC | COMMITTED => {
                    let (prefix, epoch) = cur.as_ref().context("no HELLO")?;
                    let d = self.epoch_dir(prefix, *epoch);
                    match t {
                        APPEND => {
                            let (seq, n) = (c.u64()?, c.u32()?);
                            let body = c.rest();
                            let p = d.join(format!("{seq:012}.rec"));
                            let mut f = OpenOptions::new().create(true).append(true).open(&p)?;
                            f.write_all(&n.to_le_bytes())?;
                            f.write_all(&(body.len() as u32).to_le_bytes())?;
                            f.write_all(body)?;
                            touched.insert(p);
                            touched.insert(d.clone());
                        }
                        SEAL => {
                            let seq = c.u64()?;
                            File::create(d.join(format!("{seq:012}.sealed")))?;
                            touched.insert(d.join(format!("{seq:012}.sealed")));
                            touched.insert(d.clone());
                        }
                        SYNC => {
                            // segments without a SEAL may have holes from the outage: drop them
                            for (seq, sealed) in list_seqs(&d)? {
                                if !sealed {
                                    let _ = fs::remove_file(d.join(format!("{seq:012}.rec")));
                                }
                            }
                            touched.insert(d.clone());
                        }
                        _ => {
                            let committed = c.u64()?;
                            for (seq, _) in list_seqs(&d)? {
                                if seq < committed {
                                    let _ = fs::remove_file(d.join(format!("{seq:012}.rec")));
                                    let _ = fs::remove_file(d.join(format!("{seq:012}.sealed")));
                                }
                            }
                        }
                    }
                }
                BARRIER => {
                    let id = c.u64()?;
                    for p in touched.drain() {
                        if let Ok(f) = File::open(&p) {
                            f.sync_all()?;
                        }
                    }
                    let mut ack = Vec::new();
                    put_u64(&mut ack, id);
                    write_frame(&mut w, ACK, &ack)?;
                    w.flush()?;
                }
                FETCH => {
                    let prefix = c.str()?;
                    let disk = self.disk(&prefix);
                    let floor = self.floor(&prefix);
                    let mut epochs: Vec<u64> = fs::read_dir(&disk)
                        .map(|rd| rd.filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok()).collect())
                        .unwrap_or_default();
                    epochs.sort();
                    for e in epochs.into_iter().filter(|e| *e >= floor) {
                        let d = self.epoch_dir(&prefix, e);
                        for (seq, sealed) in list_seqs(&d)? {
                            let data = fs::read(d.join(format!("{seq:012}.rec"))).unwrap_or_default();
                            let mut rc = Cursor(&data);
                            while !rc.0.is_empty() {
                                let (n, len) = (rc.u32()?, rc.u32()? as usize);
                                let body = rc.take(len)?;
                                let mut f = Vec::with_capacity(body.len() + 21);
                                put_u64(&mut f, e);
                                put_u64(&mut f, seq);
                                f.push(sealed as u8);
                                f.extend_from_slice(&n.to_le_bytes());
                                f.extend_from_slice(body);
                                write_frame(&mut w, ITEM, &f)?;
                            }
                            if data.is_empty() && sealed {
                                // a sealed segment whose data arrived in earlier batches only
                            }
                        }
                    }
                    write_frame(&mut w, END, &[])?;
                    w.flush()?;
                }
                DROP => {
                    let (prefix, below) = (c.str()?, c.u64()?);
                    let disk = self.disk(&prefix);
                    fs::create_dir_all(&disk)?;
                    if let Ok(rd) = fs::read_dir(&disk) {
                        for e in rd.flatten() {
                            if e.file_name().to_str().and_then(|n| n.parse::<u64>().ok()).is_some_and(|n| n < below) {
                                fs::remove_dir_all(e.path())?;
                            }
                        }
                    }
                    let tmp = disk.join("epoch.tmp");
                    fs::write(&tmp, below.max(self.floor(&prefix)).to_string())?;
                    File::open(&tmp)?.sync_all()?;
                    fs::rename(&tmp, disk.join("epoch"))?;
                    File::open(&disk)?.sync_all()?;
                    write_frame(&mut w, ACK, &0u64.to_le_bytes())?;
                    w.flush()?;
                }
                _ => bail!("unknown frame type {t}"),
            }
        }
    }
}

/// `(seq, sealed)` of the segments in an epoch directory, ascending.
fn list_seqs(d: &Path) -> Result<Vec<(u64, bool)>> {
    let mut m: BTreeMap<u64, bool> = BTreeMap::new();
    let Ok(rd) = fs::read_dir(d) else { return Ok(Vec::new()) };
    for e in rd.flatten() {
        let name = e.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(s) = name.strip_suffix(".rec").and_then(|s| s.parse().ok()) {
            m.entry(s).or_insert(false);
        } else if let Some(s) = name.strip_suffix(".sealed").and_then(|s| s.parse().ok()) {
            m.insert(s, true);
        }
    }
    Ok(m.into_iter().collect())
}

// ---------------------------------------------------------------------------------------------
// Taking a disk over

/// One journal batch: part of segment `seq` of `epoch` (sealed = the standby has its SEAL).
pub struct Item {
    pub epoch: u64,
    pub seq: u64,
    pub sealed: bool,
    pub n: u32,
    pub body: Vec<u8>,
}

fn connect(addr: &str) -> Result<TcpStream> {
    let sa = std::net::ToSocketAddrs::to_socket_addrs(addr)?.next().with_context(|| format!("resolve {addr}"))?;
    let s = TcpStream::connect_timeout(&sa, Duration::from_secs(3)).with_context(|| format!("connect to replica {addr}"))?;
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(Duration::from_secs(30)))?;
    Ok(s)
}

/// Everything the standby holds for `prefix`.
pub fn fetch(addr: &str, prefix: &str) -> Result<Vec<Item>> {
    let s = connect(addr)?;
    let mut w = BufWriter::new(s.try_clone()?);
    let mut p = Vec::new();
    put_str(&mut p, prefix);
    write_frame(&mut w, FETCH, &p)?;
    w.flush()?;
    let mut r = BufReader::new(s);
    let mut out = Vec::new();
    loop {
        let (t, payload) = read_frame(&mut r)?;
        match t {
            ITEM => {
                let mut c = Cursor(&payload);
                let (epoch, seq, sealed, n) = (c.u64()?, c.u64()?, c.u8()? != 0, c.u32()?);
                out.push(Item { epoch, seq, sealed, n, body: c.rest().to_vec() });
            }
            END => return Ok(out),
            _ => bail!("unexpected frame {t} from replica"),
        }
    }
}

/// Drop the journal of every epoch below `below` (they are in the object store now).
pub fn drop_below(addr: &str, prefix: &str, below: u64) -> Result<()> {
    let s = connect(addr)?;
    let mut w = BufWriter::new(s.try_clone()?);
    let mut p = Vec::new();
    put_str(&mut p, prefix);
    put_u64(&mut p, below);
    write_frame(&mut w, DROP, &p)?;
    w.flush()?;
    let (t, _) = read_frame(&mut BufReader::new(s))?;
    ensure!(t == ACK, "replica refused DROP");
    Ok(())
}

/// The name a batch is sealed under (encrypted disks): binds it to its place in the log.
pub fn batch_name(prefix: &str, epoch: u64, seq: u64, n: u32) -> String {
    format!("{prefix}/journal/{epoch:010}/{seq:012}/{n}")
}

/// Serialize builder entries `[from..]` (data entries carry their bytes).
pub fn encode_delta(b: &crate::segment::Builder, from: usize) -> Vec<u8> {
    let mut v = Vec::new();
    let es = &b.entries[from..];
    v.extend_from_slice(&(es.len() as u32).to_le_bytes());
    for e in es {
        v.push(e.kind);
        put_u64(&mut v, e.off);
        v.extend_from_slice(&e.len.to_le_bytes());
    }
    for e in es.iter().filter(|e| e.kind == crate::segment::KIND_DATA) {
        v.extend_from_slice(&b.data[e.data_off as usize..e.data_off as usize + e.len as usize]);
    }
    v
}

/// Append a serialized delta to a builder.
pub fn apply_delta(b: &mut crate::segment::Builder, delta: &[u8]) -> Result<()> {
    let mut c = Cursor(delta);
    let n = c.u32()? as usize;
    let mut es = Vec::with_capacity(n);
    for _ in 0..n {
        es.push((c.u8()?, c.u64()?, c.u32()?));
    }
    for (kind, off, len) in es {
        if kind == crate::segment::KIND_DATA {
            let d = c.take(len as usize)?.to_vec();
            b.push_data(off, &d);
        } else {
            b.push_discard(off, len);
        }
    }
    ensure!(c.0.is_empty(), "trailing bytes in a journal batch");
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The primary's side

enum Out {
    Append { seq: u64, n: u32, body: Vec<u8> },
    Seal { seq: u64 },
    Committed(u64),
    Barrier { id: u64, sealed_up_to: Option<u64> },
}

struct State {
    queue: VecDeque<Out>,
    next_id: u64,
    acked: u64,
    /// seq+1 of the highest segment whose SEAL the standby acknowledged
    sealed_acked: u64,
    /// segments below this were sealed while degraded: they upload without waiting for the standby
    ungated_below: u64,
    degraded: bool,
    waiters: Vec<(u64, RawFd)>,
    in_flight: HashMap<u64, Option<u64>>, // barrier id -> segment it seals
}

/// Called after each (re)connection with an `emit(seq, batch number, body)` for the open segment's
/// content; returns the open segment's seq.
pub type Resync<'a> = &'a dyn Fn(&mut dyn FnMut(u64, u32, Vec<u8>)) -> u64;

pub struct Replicator {
    addr: String,
    prefix: String,
    epoch: u64,
    st: Mutex<State>,
    cv: Condvar,
}

impl Replicator {
    pub fn new(addr: &str, prefix: &str, epoch: u64) -> Arc<Replicator> {
        Arc::new(Replicator {
            addr: addr.to_string(),
            prefix: prefix.to_string(),
            epoch,
            // until the first connection, behave as degraded: nothing is held back
            st: Mutex::new(State {
                queue: VecDeque::new(),
                next_id: 1,
                acked: 0,
                sealed_acked: 0,
                ungated_below: 0,
                degraded: true,
                waiters: Vec::new(),
                in_flight: HashMap::new(),
            }),
            cv: Condvar::new(),
        })
    }

    /// Queue part of segment `seq` (call under the open-segment lock, in log order).
    pub fn append(&self, seq: u64, n: u32, body: Vec<u8>) {
        let mut s = self.st.lock().unwrap();
        if !s.degraded {
            s.queue.push_back(Out::Append { seq, n, body });
            self.cv.notify_all();
        }
    }

    /// Queue a SEAL for `seq` and a barrier; uploads of `seq` wait for its acknowledgement.
    pub fn seal(&self, seq: u64) {
        let mut s = self.st.lock().unwrap();
        if s.degraded {
            s.ungated_below = s.ungated_below.max(seq + 1);
            return;
        }
        s.queue.push_back(Out::Seal { seq });
        let id = s.next_id;
        s.next_id += 1;
        s.queue.push_back(Out::Barrier { id, sealed_up_to: Some(seq) });
        self.cv.notify_all();
    }

    /// Queue a barrier; returns its id, or None when degraded (the caller must not rely on it).
    pub fn barrier(&self) -> Option<u64> {
        let mut s = self.st.lock().unwrap();
        if s.degraded {
            return None;
        }
        let id = s.next_id;
        s.next_id += 1;
        s.queue.push_back(Out::Barrier { id, sealed_up_to: None });
        self.cv.notify_all();
        Some(id)
    }

    /// Signal `efd` once barrier `id` is acknowledged, or the link fails.
    pub fn wait(&self, id: u64, efd: RawFd) {
        let mut s = self.st.lock().unwrap();
        if s.acked >= id || s.degraded {
            crate::dev::signal(efd);
        } else {
            s.waiters.push((id, efd));
        }
    }

    pub fn acked(&self, id: u64) -> bool {
        self.st.lock().unwrap().acked >= id
    }

    /// May segment `seq` be uploaded?
    pub fn may_upload(&self, seq: u64) -> bool {
        let s = self.st.lock().unwrap();
        s.degraded || seq < s.ungated_below || seq < s.sealed_acked
    }

    pub fn committed(&self, c: u64) {
        let mut s = self.st.lock().unwrap();
        if !s.degraded {
            s.queue.retain(|o| !matches!(o, Out::Committed(_)));
            s.queue.push_back(Out::Committed(c));
            self.cv.notify_all();
        }
    }

    fn degrade(&self, why: &str) {
        let mut s = self.st.lock().unwrap();
        if !s.degraded {
            crate::warn!("disk {}: replica {} unavailable ({why}); flushes wait for the object store", self.prefix, self.addr);
        }
        s.degraded = true;
        s.queue.clear();
        s.in_flight.clear();
        for (_, fd) in std::mem::take(&mut s.waiters) {
            crate::dev::signal(fd);
        }
    }

    /// The sender thread. `resync` is called after each (re)connection, under the open-segment
    /// lock: it returns the open segment's seq and its content so far, and must reset the caller's
    /// "sent" cursor to the end of it.
    pub fn run(self: &Arc<Self>, resync: impl Fn(&mut dyn FnMut(u64, u32, Vec<u8>)) -> u64, stop: impl Fn() -> bool) {
        while !stop() {
            let s = match connect(&self.addr) {
                Ok(s) => s,
                Err(e) => {
                    self.degrade(&format!("{e:#}"));
                    std::thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };
            if let Err(e) = self.session(s, &resync, &stop) {
                self.degrade(&format!("{e:#}"));
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }

    /// Leave degraded mode: called by `resync` while it holds the open-segment lock, so that every
    /// append queued afterwards follows the full content it captured.
    pub fn go_live(&self, open_seq: u64) {
        let mut st = self.st.lock().unwrap();
        st.queue.clear();
        st.ungated_below = st.ungated_below.max(open_seq);
        st.degraded = false;
    }

    /// Segments below this were sealed while degraded: a flush may rely on the standby only once
    /// they are committed.
    pub fn ungated_below(&self) -> u64 {
        self.st.lock().unwrap().ungated_below
    }

    fn session(self: &Arc<Self>, s: TcpStream, resync: Resync, stop: &dyn Fn() -> bool) -> Result<()> {
        s.set_read_timeout(Some(ACK_TIMEOUT))?;
        let mut w = BufWriter::new(s.try_clone()?);
        let mut hello = Vec::new();
        put_str(&mut hello, &self.prefix);
        put_u64(&mut hello, self.epoch);
        write_frame(&mut w, HELLO, &hello)?;
        // SYNC and the open segment in full, then leave degraded mode: from here on the journal of
        // this epoch is complete for every segment that gets a SEAL
        {
            let mut first: Vec<(u64, u32, Vec<u8>)> = Vec::new();
            let open_seq = resync(&mut |seq, n, body| first.push((seq, n, body)));
            let mut p = Vec::new();
            put_u64(&mut p, open_seq);
            write_frame(&mut w, SYNC, &p)?;
            for (seq, n, body) in first {
                let mut f = Vec::with_capacity(body.len() + 12);
                put_u64(&mut f, seq);
                f.extend_from_slice(&n.to_le_bytes());
                f.extend_from_slice(&body);
                write_frame(&mut w, APPEND, &f)?;
            }
        }
        w.flush()?;
        crate::info!("disk {}: replicating to {}", self.prefix, self.addr);
        let reader = s.try_clone()?;
        let me = self.clone();
        let acks = std::thread::spawn(move || -> Result<()> {
            let mut r = BufReader::new(reader);
            loop {
                let (t, payload) = match read_frame(&mut r) {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                        let st = me.st.lock().unwrap();
                        if st.degraded {
                            return Ok(());
                        }
                        if !st.in_flight.is_empty() {
                            bail!("no acknowledgement for {ACK_TIMEOUT:?}");
                        }
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                };
                ensure!(t == ACK, "replica sent frame {t} ({})", String::from_utf8_lossy(&payload));
                let id = u64::from_le_bytes(payload.get(..8).context("short ack")?.try_into()?);
                let mut st = me.st.lock().unwrap();
                if let Some(Some(seq)) = st.in_flight.remove(&id) {
                    st.sealed_acked = st.sealed_acked.max(seq + 1);
                }
                st.in_flight.retain(|k, _| *k > id);
                st.acked = st.acked.max(id);
                let acked = st.acked;
                st.waiters.retain(|(i, fd)| {
                    if *i <= acked {
                        crate::dev::signal(*fd);
                        false
                    } else {
                        true
                    }
                });
            }
        });
        let res = (|| -> Result<()> {
            loop {
                let next = {
                    let mut st = self.st.lock().unwrap();
                    loop {
                        if st.degraded || stop() {
                            return Ok(());
                        }
                        if acks.is_finished() {
                            bail!("acknowledgements stopped");
                        }
                        if let Some(o) = st.queue.pop_front() {
                            if let Out::Barrier { id, sealed_up_to } = &o {
                                st.in_flight.insert(*id, *sealed_up_to);
                            }
                            break o;
                        }
                        st = self.cv.wait_timeout(st, Duration::from_millis(200)).unwrap().0;
                    }
                };
                match next {
                    Out::Append { seq, n, body } => {
                        let mut f = Vec::with_capacity(body.len() + 12);
                        put_u64(&mut f, seq);
                        f.extend_from_slice(&n.to_le_bytes());
                        f.extend_from_slice(&body);
                        write_frame(&mut w, APPEND, &f)?;
                    }
                    Out::Seal { seq } => write_frame(&mut w, SEAL, &seq.to_le_bytes())?,
                    Out::Committed(c) => write_frame(&mut w, COMMITTED, &c.to_le_bytes())?,
                    Out::Barrier { id, .. } => {
                        write_frame(&mut w, BARRIER, &id.to_le_bytes())?;
                        w.flush()?;
                    }
                }
                if self.st.lock().unwrap().queue.is_empty() {
                    w.flush()?;
                }
            }
        })();
        let _ = s.shutdown(std::net::Shutdown::Both);
        let ack_res = acks.join().unwrap_or_else(|_| Err(anyhow::anyhow!("ack thread panicked")));
        res.and(ack_res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::Builder;

    #[test]
    fn deltas_rebuild_the_builder() {
        let mut b = Builder::default();
        b.push_data(0, &[1u8; 4096]);
        b.push_discard(8192, 4096);
        let first = encode_delta(&b, 0);
        b.push_data(4096, &[2u8; 8192]);
        let second = encode_delta(&b, 2);
        let mut r = Builder::default();
        apply_delta(&mut r, &first).unwrap();
        apply_delta(&mut r, &second).unwrap();
        assert_eq!(r.entries, b.entries);
        assert_eq!(r.data, b.data);
        assert!(apply_delta(&mut r, &first[..first.len() - 1]).is_err());
    }

    #[test]
    fn journal_serves_appends_seals_and_drops() {
        let dir = std::env::temp_dir().join(format!("blockgw-replica-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let j = Arc::new(Journal { root: dir.clone(), lock: Mutex::new(()) });
        let j2 = j.clone();
        std::thread::spawn(move || {
            for s in listener.incoming().flatten() {
                let j = j2.clone();
                std::thread::spawn(move || {
                    let _ = j.session(s);
                });
            }
        });
        let s = connect(&addr).unwrap();
        let mut w = BufWriter::new(s.try_clone().unwrap());
        let mut r = BufReader::new(s);
        let mut hello = Vec::new();
        put_str(&mut hello, "p/d");
        put_u64(&mut hello, 3);
        write_frame(&mut w, HELLO, &hello).unwrap();
        for (seq, n, body) in [(0u64, 0u32, b"a".to_vec()), (0, 1, b"b".to_vec()), (1, 0, b"c".to_vec()), (2, 0, b"x".to_vec())] {
            let mut f = Vec::new();
            put_u64(&mut f, seq);
            f.extend_from_slice(&n.to_le_bytes());
            f.extend_from_slice(&body);
            write_frame(&mut w, APPEND, &f).unwrap();
        }
        write_frame(&mut w, SEAL, &0u64.to_le_bytes()).unwrap();
        write_frame(&mut w, SEAL, &1u64.to_le_bytes()).unwrap();
        write_frame(&mut w, BARRIER, &9u64.to_le_bytes()).unwrap();
        w.flush().unwrap();
        let (t, p) = read_frame(&mut r).unwrap();
        assert_eq!((t, u64::from_le_bytes(p[..8].try_into().unwrap())), (ACK, 9));
        let items = fetch(&addr, "p/d").unwrap();
        let shape: Vec<(u64, u64, bool, u32, Vec<u8>)> = items.iter().map(|i| (i.epoch, i.seq, i.sealed, i.n, i.body.clone())).collect();
        assert_eq!(
            shape,
            vec![
                (3, 0, true, 0, b"a".to_vec()),
                (3, 0, true, 1, b"b".to_vec()),
                (3, 1, true, 0, b"c".to_vec()),
                (3, 2, false, 0, b"x".to_vec())
            ]
        );
        // COMMITTED trims, SYNC drops the unsealed segment
        write_frame(&mut w, COMMITTED, &1u64.to_le_bytes()).unwrap();
        write_frame(&mut w, SYNC, &2u64.to_le_bytes()).unwrap();
        write_frame(&mut w, BARRIER, &10u64.to_le_bytes()).unwrap();
        w.flush().unwrap();
        read_frame(&mut r).unwrap();
        let items = fetch(&addr, "p/d").unwrap();
        assert_eq!(items.iter().map(|i| (i.seq, i.sealed)).collect::<Vec<_>>(), vec![(1, true)]);
        // DROP: gone, and the old epoch cannot come back
        drop_below(&addr, "p/d", 4).unwrap();
        assert!(fetch(&addr, "p/d").unwrap().is_empty());
        let s2 = connect(&addr).unwrap();
        let mut w2 = BufWriter::new(s2.try_clone().unwrap());
        write_frame(&mut w2, HELLO, &hello).unwrap();
        w2.flush().unwrap();
        let (t, _) = read_frame(&mut BufReader::new(s2)).unwrap();
        assert_eq!(t, REFUSED);
        let _ = fs::remove_dir_all(&dir);
    }
}
