#!/usr/bin/env python3
"""blockgw crash-consistency torture test (needs root, ublk_drv, a built blockgw).

Writer threads (`--threads`, default 1) issue O_DIRECT 4 KiB writes to random blocks of a blockgw
device, each thread one write at a time to blocks only it owns, with random fsyncs. Each block
written carries (block, global write id, checksum). Meanwhile:
  * the worker process is killed with SIGKILL at random intervals (the supervisor restarts it),
  * uploads fail at random (failpoint `upload=error%P`; they are retried),
  * the local-directory object store is copied at random moments. A copy is what a host that died
    at that instant would leave behind in the object store,
  * checkpoints are taken every 8 segments, so garbage collection deletes and repacks segments
    throughout.

Checks:
  1. no acknowledged write is lost across worker crashes: the device ends up holding, for every
     block, the last version written;
  2. every store copy restores to a crash-consistent image (read through the restored device, so
     lazy on-demand fetches are exercised), with no torn block:
     - happens-before: if the copy holds write W, it holds every write that completed before W was
       issued (or a later write to the same block);
     - with one thread, writes are totally ordered, so this tightens to: the image equals the state
       after exactly the first K writes, for some K;
  3. under the `remote` policy, the copy holds every write that completed before the start of an
     fsync that returned before the copy started.

Usage: sudo tests/torture/torture.py --blockgw target/release/blockgw [--policy local|remote]
                                     [--threads 1] [--encrypt] [--seconds 60] [--seed N] [--id 1500] [--work /tmp/blockgw-torture]
"""
import argparse, json, mmap, os, random, shutil, signal, struct, subprocess, sys, threading, time, zlib

BS = 4096
NBLOCKS = 16384  # 64 MiB working set at the start of a 1 GiB disk
HDR = struct.Struct("<4sIQI")  # magic, block, gid, crc32(payload)


def block_bytes(block, gid):
    payload = random.Random(gid * 1_000_003 + block).randbytes(BS - HDR.size)
    return HDR.pack(b"TORT", block, gid, zlib.crc32(payload)) + payload


def parse_block(buf, block):
    """gid stored in a block (0 = never written); raises on a torn or misplaced block."""
    if buf == b"\0" * BS:
        return 0
    magic, b, gid, crc = HDR.unpack_from(buf)
    if magic != b"TORT" or b != block or zlib.crc32(buf[HDR.size:]) != crc:
        raise AssertionError(f"block {block}: torn or foreign content (magic={magic!r} block={b} gid={gid})")
    return gid


def log(msg):
    print(f"# {time.strftime('%H:%M:%S')} {msg}", flush=True)


class Server:
    def __init__(self, blockgw, cfg, args=(), env=None):
        self.cfg, self.log = cfg, cfg.replace(".json", ".log")
        self.proc = subprocess.Popen([blockgw, "serve", cfg, *args], stdout=open(self.log, "ab"), stderr=subprocess.STDOUT,
                                     env={**os.environ, **(env or {})}, start_new_session=True)

    def ready_count(self, dev_id):
        with open(self.log, "rb") as f:
            return f.read().count(f"/dev/ublkb{dev_id} ready".encode())

    def wait_ready(self, dev_id, at_least=1, timeout=120):
        t0 = time.time()
        while self.ready_count(dev_id) < at_least:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited: {open(self.log).read()[-2000:]}")
            if time.time() - t0 > timeout:
                raise RuntimeError("device not ready")
            time.sleep(0.05)

    def worker_pid(self):
        out = subprocess.run(["pgrep", "-P", str(self.proc.pid), "-f", "blockgw worker"], capture_output=True, text=True).stdout.split()
        return int(out[0]) if out else None


def dev_node(dev_id, timeout=10):
    """/dev/ublkbN, waiting for udev; without udev (e.g. a container's private /dev) create it."""
    path, t0 = f"/dev/ublkb{dev_id}", time.time()
    while not os.path.exists(path):
        if time.time() - t0 > timeout:
            major, minor = map(int, open(f"/sys/block/ublkb{dev_id}/dev").read().split(":"))
            os.mknod(path, 0o600 | 0o060000, os.makedev(major, minor))  # S_IFBLK
            break
        time.sleep(0.05)
    return path


def copy_store(src, dst):
    """Copy a live store so that the copy is a state the store really passed through.

    Objects are only ever created (immutable) or deleted by GC. GC deletes old checkpoints before
    segments, so while a checkpoint exists every segment it references exists, and segments after
    the newest checkpoint are never deleted. So: list the checkpoints, copy the segments (skipping
    any that vanish meanwhile), then the listed checkpoints. If the newest listed checkpoint still
    exists afterwards, GC never deleted anything the copy needs; with none listed, there must still
    be none (no checkpoint, no GC). Otherwise (checkpoints were written during the copy) start over.
    """
    def files(root):
        for dirpath, _, names in os.walk(root):
            for n in names:
                if not n.startswith(".tmp."):
                    yield os.path.join(dirpath, n)

    def cp(path):
        rel = os.path.relpath(path, src)
        os.makedirs(os.path.join(dst, os.path.dirname(rel)), exist_ok=True)
        try:
            shutil.copyfile(path, os.path.join(dst, rel))
        except FileNotFoundError:
            pass  # deleted by GC after the listing

    for _ in range(20):
        ckpts = [p for p in files(src) if "/ckpt/" in p]
        for p in files(src):
            if "/ckpt/" not in p:
                cp(p)
        for p in ckpts:
            cp(p)
        newest = max(ckpts, default=None)  # keys are zero-padded: string order is log order
        if newest is not None and os.path.exists(newest):
            return
        if newest is None and not any("/ckpt/" in p for p in files(src)):
            return  # no checkpoint yet, so no GC either
        shutil.rmtree(dst)
    raise RuntimeError("could not take a consistent store copy in 20 attempts")


def write_config(path, store, image_dir, dev_id, policy, key_file=None):
    dev = {"id": dev_id, "size_gb": 1, "dir": image_dir, "prefix": "torture/d", "policy": policy,
           "checkpoint_every": 8}  # frequent checkpoints keep GC busy
    if key_file:
        dev["key_file"] = key_file
    json.dump({"local_store": store, "control_socket": path.replace(".json", ".sock"), "upload_workers": 8,
               "devices": [dev]}, open(path, "w"))


def delete_device(blockgw, dev_id):
    if os.path.exists(f"/sys/class/ublk-char/ublkc{dev_id}"):
        subprocess.run([blockgw, "del", str(dev_id)], capture_output=True, timeout=30)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blockgw", required=True)
    ap.add_argument("--policy", default="local", choices=["local", "remote"])
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--seed", type=int, default=int(time.time()))
    ap.add_argument("--id", type=int, default=1500)
    ap.add_argument("--work", default="/tmp/blockgw-torture")
    ap.add_argument("--upload-error-pct", type=int, default=10)
    ap.add_argument("--encrypt", action="store_true", help="encrypt the disk (key_file)")
    ap.add_argument("--threads", type=int, default=1, help="concurrent writer threads")
    a = ap.parse_args()
    blockgw = os.path.abspath(a.blockgw)
    rng = random.Random(a.seed)
    work = os.path.join(a.work, f"{a.policy}{'-enc' if a.encrypt else ''}-t{a.threads}-{a.seed}")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    log(f"seed={a.seed} policy={a.policy} threads={a.threads} encrypt={a.encrypt} seconds={a.seconds} work={work}")
    key_file = None
    if a.encrypt:
        key_file = os.path.join(work, "disk.key")
        with open(os.open(key_file, os.O_WRONLY | os.O_CREAT, 0o600), "wb") as f:
            f.write(os.urandom(32))

    store = os.path.join(work, "store")
    cfg = os.path.join(work, "main.json")
    write_config(cfg, store, os.path.join(work, "img"), a.id, a.policy, key_file)
    srv = Server(blockgw, cfg, ["--orphan-timeout", "120"], {"BLOCKGW_FAILPOINTS": f"upload=error%{a.upload_error_pct}"})
    used_ids = [a.id]
    failures = []
    try:
        srv.wait_ready(a.id)
        fd = os.open(dev_node(a.id), os.O_RDWR | os.O_DIRECT)
        writes = []           # index gid-1: [block, t_issue, t_done]
        wlock = threading.Lock()
        state = {"cut": 0.0}  # writes done before this instant are covered by a completed fsync
        snaps = []            # (dir, cut when the copy started)
        stop = threading.Event()
        kills = [0]

        def killer():
            while not stop.wait(rng.uniform(0.5, 2.0)):
                pid = srv.worker_pid()
                if pid:
                    before = srv.ready_count(a.id)
                    os.kill(pid, signal.SIGKILL)
                    kills[0] += 1
                    srv.wait_ready(a.id, before + 1)

        def snapshotter():
            n = 0
            while not stop.wait(rng.uniform(1.0, 3.0)):
                cut = state["cut"]
                dst = os.path.join(work, f"snap{n}")
                copy_store(store, dst)
                snaps.append((dst, cut))
                n += 1

        threads = [threading.Thread(target=killer, daemon=True), threading.Thread(target=snapshotter, daemon=True)]
        for t in threads:
            t.start()
        t_end = time.time() + a.seconds

        def writer(i):
            wr = random.Random(a.seed * 1000 + i)
            buf = mmap.mmap(-1, BS)
            while time.time() < t_end:
                block = wr.randrange(NBLOCKS // a.threads) * a.threads + i  # blocks owned by this thread
                with wlock:
                    writes.append([block, time.monotonic(), None])
                    gid = len(writes)
                buf.seek(0)
                buf.write(block_bytes(block, gid))
                os.pwrite(fd, buf, block * BS)
                writes[gid - 1][2] = time.monotonic()
                if wr.random() < 0.05:
                    t0 = time.monotonic()
                    os.fsync(fd)
                    with wlock:
                        state["cut"] = max(state["cut"], t0)

        wts = [threading.Thread(target=writer, args=(i,)) for i in range(a.threads)]
        for t in wts:
            t.start()
        for t in wts:
            t.join()
        t0 = time.monotonic()
        os.fsync(fd)
        state["cut"] = t0
        stop.set()
        for t in threads:
            t.join()
        log(f"{len(writes)} writes, {kills[0]} worker kills, {len(snaps)} store copies")

        # 1. the device holds the last version of every block
        last = {}
        for i, (b, _, _) in enumerate(writes):
            last[b] = i + 1
        dev = b"".join(os.pread(fd, BS * 1024, off) for off in range(0, NBLOCKS * BS, BS * 1024))
        bad = [b for b in range(NBLOCKS) if parse_block(dev[b * BS:(b + 1) * BS], b) != last.get(b, 0)]
        if bad:
            failures.append(f"device lost acknowledged writes in {len(bad)} blocks, e.g. {bad[:5]}")
        else:
            log("check 1 ok: no write lost across worker crashes")
        os.close(fd)
        subprocess.run([blockgw, "ctl", cfg.replace(".json", ".sock"), "drain", str(a.id)], capture_output=True)

        # 2 + 3. every store copy restores to a crash-consistent state
        by_done = sorted(range(len(writes)), key=lambda g: writes[g][2])  # gid-1 by completion time
        for k, (snap, cut) in enumerate(snaps):
            rid = a.id + 1 + k
            used_ids.append(rid)
            rcfg = os.path.join(work, f"restore{k}.json")
            rimg = os.path.join(work, f"rimg{k}")
            write_config(rcfg, snap, rimg, rid, "local", key_file)
            r = Server(blockgw, rcfg, ["--no-supervisor"])
            try:
                r.wait_ready(rid)
                # read through the device: after a lazy restore this fetches blocks on demand
                rfd = os.open(dev_node(rid), os.O_RDONLY | os.O_DIRECT)
                try:
                    img = b"".join(os.pread(rfd, BS * 1024, off) for off in range(0, NBLOCKS * BS, BS * 1024))
                finally:
                    os.close(rfd)
                gids = [parse_block(img[b * BS:(b + 1) * BS], b) for b in range(NBLOCKS)]
                K = max(gids)
                problems = []
                if a.threads == 1:
                    expect = {}
                    for i, (b, _, _) in enumerate(writes[:K]):
                        expect[b] = i + 1
                    wrong = [b for b in range(NBLOCKS) if gids[b] != expect.get(b, 0)]
                    if wrong:
                        problems.append(f"not the state after the first {K} writes ({len(wrong)} blocks differ, e.g. {wrong[:5]})")
                # happens-before closure, and the remote fsync guarantee: every write done before
                # `need` must be held (per block, gids grow with time: one thread owns each block)
                newest_issue = max((writes[g - 1][1] for g in gids if g), default=0.0)
                need = max(newest_issue, cut if a.policy == "remote" else 0.0)
                missing = []
                for g in by_done:
                    b, _, done = writes[g]
                    if done >= need:
                        break
                    if gids[b] < g + 1:
                        missing.append(g + 1)
                if missing:
                    why = "acknowledged by an fsync before the copy, or done before a write the copy holds was issued"
                    problems.append(f"{len(missing)} writes missing that were {why}, e.g. gids {missing[:5]}")
                if problems:
                    failures.append(f"copy {k}: " + "; ".join(problems))
                else:
                    held = sum(1 for g in gids if g)
                    log(f"copy {k}: crash-consistent ({held} blocks written, newest gid {K})")
            except AssertionError as e:
                failures.append(f"copy {k}: {e}")
            finally:
                delete_device(blockgw, rid)
                r.proc.wait(timeout=30)
    finally:
        for i in used_ids:
            delete_device(blockgw, i)
        try:
            srv.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            srv.proc.kill()
    if failures:
        for f in failures:
            print(f"FAIL: {f}")
        print(f"# reproduce with --seed {a.seed} --policy {a.policy}")
        sys.exit(1)
    log("all checks passed")


if __name__ == "__main__":
    main()
