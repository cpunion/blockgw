#!/usr/bin/env python3
"""blockgw filesystem-level crash-consistency torture (needs root, ublk_drv, xfsprogs, blockgw).

XFS runs on a blockgw device while worker threads create, overwrite, rename, truncate and delete
files, fsyncing some of them. Meanwhile the worker process is killed at random, uploads fail at
random, and the object store is copied at random moments (the same faults as torture.py).

XFS depends on the block layer honouring flush ordering: data that completed before a flush must
be durable before anything written after it. blockgw logs writes in arrival order and every store
copy restores to a prefix of that log, so every copy must hold a consistent filesystem.

Checks:
  1. the live filesystem is clean after the run (`xfs_repair -n` after unmount);
  2. every store copy restores to a filesystem that mounts (replaying its log) and that
     `xfs_repair -n` then finds clean;
  3. under the `remote` policy, every file whose fsync (file and directory) returned before the
     copy started, and that nobody touched until the copy ended, is in the copy with its content.

Usage: sudo tests/torture/fstorture.py --blockgw target/release/blockgw [--policy local|remote]
                                       [--threads 4] [--encrypt] [--seconds 60] [--seed N]
"""
import argparse, hashlib, os, random, shutil, signal, subprocess, sys, threading, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from torture import Server, copy_store, delete_device, dev_node, log, write_config  # noqa: E402


def sh(*cmd, check=True):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        kmsg = subprocess.run(["dmesg"], capture_output=True, text=True).stdout.strip().splitlines()[-8:]
        raise AssertionError(f"{' '.join(cmd)} failed ({r.returncode}): {(r.stdout + r.stderr).strip()[-500:]}\n  kernel: " + "\n  kernel: ".join(kmsg))
    return r


def repair_problems(r):
    """The lines of an `xfs_repair -n` run that describe problems (not progress)."""
    lines = (r.stdout + r.stderr).splitlines()
    bad = [l.strip() for l in lines if l.strip() and not l.startswith(("Phase", "        - ")) and "host filesystem geometry" not in l
           and "sector size mismatch" not in l and not l.startswith("No modify flag")]
    return "; ".join(bad[:12])


def content(seed, size):
    return random.Random(seed).randbytes(size)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blockgw", required=True)
    ap.add_argument("--policy", default="local", choices=["local", "remote"])
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--seed", type=int, default=int(time.time()))
    ap.add_argument("--id", type=int, default=1700)
    ap.add_argument("--work", default="/tmp/blockgw-fstorture")
    ap.add_argument("--upload-error-pct", type=int, default=10)
    ap.add_argument("--encrypt", action="store_true")
    a = ap.parse_args()
    blockgw = os.path.abspath(a.blockgw)
    rng = random.Random(a.seed)
    work = os.path.join(a.work, f"{a.policy}{'-enc' if a.encrypt else ''}-{a.seed}")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    log(f"seed={a.seed} policy={a.policy} threads={a.threads} encrypt={a.encrypt} seconds={a.seconds} work={work}")
    key_file = None
    if a.encrypt:
        key_file = os.path.join(work, "disk.key")
        with open(os.open(key_file, os.O_WRONLY | os.O_CREAT, 0o600), "wb") as f:
            f.write(os.urandom(32))

    store, cfg, mnt = os.path.join(work, "store"), os.path.join(work, "main.json"), os.path.join(work, "mnt")
    write_config(cfg, store, os.path.join(work, "img"), a.id, a.policy, key_file)
    srv = Server(blockgw, cfg, ["--orphan-timeout", "120"], {"BLOCKGW_FAILPOINTS": f"upload=error%{a.upload_error_pct}"})
    used_ids, failures, mounts = [a.id], [], []
    try:
        srv.wait_ready(a.id)
        dev = dev_node(a.id)
        sh("mkfs.xfs", "-q", "-f", dev)
        os.makedirs(mnt)
        sh("mount", "-o", "noatime", dev, mnt)
        mounts.append(mnt)

        # path -> list of events (t_start, t_end, kind, digest); kind "durable" carries the digest
        events, elock = {}, threading.Lock()
        snaps, stop, kills, ops = [], threading.Event(), [0], [0]

        def note(path, t0, t1, kind, digest=None):
            with elock:
                events.setdefault(path, []).append((t0, t1, kind, digest))

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
                t0 = time.monotonic()
                dst = os.path.join(work, f"snap{n}")
                copy_store(store, dst)
                snaps.append((dst, t0, time.monotonic()))
                n += 1

        t_end = time.time() + a.seconds

        def worker(i):
            wr = random.Random(a.seed * 1000 + i)
            base = os.path.join(mnt, f"w{i}")
            os.makedirs(base)
            live = []
            n = 0
            while time.time() < t_end:
                op = wr.random()
                t0 = time.monotonic()
                if op < 0.35 or not live:  # create (sometimes in a new subdirectory), maybe fsync
                    d = os.path.join(base, f"d{wr.randrange(8)}")
                    os.makedirs(d, exist_ok=True)
                    p = os.path.join(d, f"f{n}")
                    n += 1
                    data = content(wr.random(), wr.choice([0, 100, 4096, 5000, 65536, 300_000]))
                    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
                    os.write(fd, data)
                    if wr.random() < 0.5:
                        os.fsync(fd)
                        os.close(fd)
                        dfd = os.open(d, os.O_RDONLY)
                        os.fsync(dfd)
                        os.close(dfd)
                        note(p, t0, time.monotonic(), "durable", hashlib.sha256(data).hexdigest())
                    else:
                        os.close(fd)
                        note(p, t0, time.monotonic(), "write")
                    live.append(p)
                elif op < 0.55:  # overwrite a range
                    p = wr.choice(live)
                    note(p, t0, t0, "write")
                    with open(p, "r+b") as f:
                        f.seek(wr.randrange(200_000))
                        f.write(content(wr.random(), wr.randrange(1, 20_000)))
                elif op < 0.65:  # truncate
                    p = wr.choice(live)
                    note(p, t0, t0, "write")
                    os.truncate(p, wr.randrange(100_000))
                elif op < 0.80:  # rename within the worker's tree
                    p = wr.choice(live)
                    q = os.path.join(base, f"d{wr.randrange(8)}", f"r{n}")
                    n += 1
                    os.makedirs(os.path.dirname(q), exist_ok=True)
                    note(p, t0, t0, "gone")
                    note(q, t0, t0, "write")
                    os.rename(p, q)
                    live[live.index(p)] = q
                else:  # unlink
                    p = live.pop(wr.randrange(len(live)))
                    note(p, t0, t0, "gone")
                    os.unlink(p)
                ops[0] += 1

        wts = [threading.Thread(target=worker, args=(i,)) for i in range(a.threads)]
        aux = [threading.Thread(target=killer, daemon=True), threading.Thread(target=snapshotter, daemon=True)]
        for t in aux + wts:
            t.start()
        for t in wts:
            t.join()
        stop.set()
        for t in aux:
            t.join()
        log(f"{ops[0]} file operations, {kills[0]} worker kills, {len(snaps)} store copies")

        # 1. the live filesystem
        sh("umount", mnt)
        mounts.remove(mnt)
        r = sh("xfs_repair", "-n", dev, check=False)
        if r.returncode != 0:
            failures.append(f"live filesystem not clean: {repair_problems(r)}")
        else:
            log("check 1 ok: live filesystem clean")
        subprocess.run([blockgw, "ctl", cfg.replace(".json", ".sock"), "drain", str(a.id)], capture_output=True)

        # 2 + 3. every store copy
        for k, (snap, c0, c1) in enumerate(snaps):
            rid = a.id + 1 + k
            used_ids.append(rid)
            rcfg, rmnt = os.path.join(work, f"restore{k}.json"), os.path.join(work, f"rmnt{k}")
            write_config(rcfg, snap, os.path.join(work, f"rimg{k}"), rid, "local", key_file)
            rs = Server(blockgw, rcfg, ["--no-supervisor"])
            try:
                rs.wait_ready(rid)
                rdev = dev_node(rid)
                os.makedirs(rmnt)
                sh("mount", "-o", "noatime,nouuid", rdev, rmnt)  # replays the log
                sh("umount", rmnt)
                r = sh("xfs_repair", "-n", rdev, check=False)
                if r.returncode != 0:
                    raise AssertionError(f"filesystem not clean after log replay: {repair_problems(r)}")
                checked = 0
                if a.policy == "remote":
                    sh("mount", "-o", "ro,noatime,nouuid", rdev, rmnt)
                    mounts.append(rmnt)
                    bad = []
                    for p, evs in events.items():
                        dur = [e for e in evs if e[2] == "durable" and e[1] < c0]
                        if not dur:
                            continue
                        last = dur[-1]
                        if any(e is not last and last[0] <= e[0] < c1 for e in evs):
                            continue  # touched again before the copy ended: either state is allowed
                        q = os.path.join(rmnt, os.path.relpath(p, mnt))
                        try:
                            with open(q, "rb") as f:
                                got = hashlib.sha256(f.read()).hexdigest()
                        except FileNotFoundError:
                            got = "missing"
                        if got != last[3]:
                            bad.append((os.path.relpath(p, mnt), got[:12]))
                        checked += 1
                    sh("umount", rmnt)
                    mounts.remove(rmnt)
                    if bad:
                        raise AssertionError(f"{len(bad)} of {checked} fsynced files lost or wrong, e.g. {bad[:3]}")
                log(f"copy {k}: filesystem clean" + (f", {checked} fsynced files intact" if a.policy == "remote" else ""))
            except AssertionError as e:
                failures.append(f"copy {k}: {e}")
            finally:
                if rmnt in mounts:
                    subprocess.run(["umount", "-l", rmnt])
                    mounts.remove(rmnt)
                delete_device(blockgw, rid)
                rs.proc.wait(timeout=30)
    finally:
        for m in mounts:
            subprocess.run(["umount", "-l", m])
        for i in used_ids:
            delete_device(blockgw, i)
        try:
            srv.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            srv.proc.kill()
    if failures:
        for f in failures:
            log(f"FAIL: {f}")
        log(f"reproduce with --seed {a.seed} --policy {a.policy} --threads {a.threads}")
        sys.exit(1)
    log("all checks passed")


if __name__ == "__main__":
    main()
