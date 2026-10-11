#!/usr/bin/env python3
"""blockgw crash-consistency torture test (needs root, ublk_drv, a built blockgw).

A single writer issues O_DIRECT 4 KiB writes to random blocks of a blockgw device, one at a time,
with random fsyncs. Each block it writes carries (block, global write id, checksum). Meanwhile:
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
     lazy on-demand fetches are exercised): because writes are issued one at a time, it must equal
     the state after exactly the first K writes for some K, with no torn block;
  3. under the `remote` policy, K covers every write acknowledged by an fsync that returned before
     the copy started.

Usage: sudo tests/torture/torture.py --blockgw target/release/blockgw [--policy local|remote]
                                     [--seconds 60] [--seed N] [--id 1500] [--work /tmp/blockgw-torture]
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

    Objects are only ever created (immutable) or deleted by GC, and GC only deletes segments that
    neither of the last two checkpoints references. So: list the checkpoints first, then copy the
    segments (skipping any that vanish meanwhile), then copy only the checkpoints listed first.
    A checkpoint written during the copy is left out; it could reference segments the copy missed.
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

    ckpts = [p for p in files(src) if "/ckpt/" in p]
    for p in files(src):
        if "/ckpt/" not in p:
            cp(p)
    for p in ckpts:
        cp(p)


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
    a = ap.parse_args()
    blockgw = os.path.abspath(a.blockgw)
    rng = random.Random(a.seed)
    work = os.path.join(a.work, f"{a.policy}{'-enc' if a.encrypt else ''}-{a.seed}")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    log(f"seed={a.seed} policy={a.policy} encrypt={a.encrypt} seconds={a.seconds} work={work}")
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
        buf = mmap.mmap(-1, BS)
        writes = []          # gid -> block (index gid-1)
        state = {"acked": 0}  # highest gid covered by a completed fsync
        snaps = []           # (dir, acked gid when the copy started)
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
                acked = state["acked"]
                dst = os.path.join(work, f"snap{n}")
                copy_store(store, dst)
                snaps.append((dst, acked))
                n += 1

        threads = [threading.Thread(target=killer, daemon=True), threading.Thread(target=snapshotter, daemon=True)]
        for t in threads:
            t.start()
        t_end = time.time() + a.seconds
        while time.time() < t_end:
            block = rng.randrange(NBLOCKS)
            gid = len(writes) + 1
            buf.seek(0)
            buf.write(block_bytes(block, gid))
            os.pwrite(fd, buf, block * BS)
            writes.append(block)
            if rng.random() < 0.05:
                os.fsync(fd)
                state["acked"] = gid
        os.fsync(fd)
        state["acked"] = len(writes)
        stop.set()
        for t in threads:
            t.join()
        log(f"{len(writes)} writes, {kills[0]} worker kills, {len(snaps)} store copies")

        # 1. the device holds the last version of every block
        last = {}
        for i, b in enumerate(writes):
            last[b] = i + 1
        dev = b"".join(os.pread(fd, BS * 1024, off) for off in range(0, NBLOCKS * BS, BS * 1024))
        bad = [b for b in range(NBLOCKS) if parse_block(dev[b * BS:(b + 1) * BS], b) != last.get(b, 0)]
        if bad:
            failures.append(f"device lost acknowledged writes in {len(bad)} blocks, e.g. {bad[:5]}")
        else:
            log("check 1 ok: no write lost across worker crashes")
        os.close(fd)
        subprocess.run([blockgw, "ctl", cfg.replace(".json", ".sock"), "drain", str(a.id)], capture_output=True)

        # 2 + 3. every store copy restores to the state after exactly K writes
        for k, (snap, acked) in enumerate(snaps):
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
                expect = {}
                for i, b in enumerate(writes[:K]):
                    expect[b] = i + 1
                wrong = [b for b in range(NBLOCKS) if gids[b] != expect.get(b, 0)]
                if wrong:
                    failures.append(f"copy {k}: not the state after the first {K} writes ({len(wrong)} blocks differ, e.g. {wrong[:5]})")
                elif a.policy == "remote" and K < acked:
                    failures.append(f"copy {k}: holds {K} writes but {acked} were acknowledged by fsync before the copy (remote policy)")
                else:
                    log(f"copy {k}: crash-consistent prefix of {K} writes (acked before copy: {acked})")
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
