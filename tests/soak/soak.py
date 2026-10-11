#!/usr/bin/env python3
"""blockgw soak test: hours of filesystem churn on a few disks, watching for slow leaks.

XFS on each disk; workers keep overwriting, appending to and deleting files within a fixed amount
of live data, with fsyncs, while the worker process is occasionally restarted. Samples, every
`--interval` seconds: worker RSS, the segment bytes it holds by design (sealed segments waiting
for upload, up to the lag limit, and the open segment), open file descriptors, threads, and per
disk the number of segment objects and their bytes in the store.

Checks at the end (after a warm-up of `--warmup` of the run):
  1. memory: worker RSS minus the segment bytes it holds by design, in the last fifth of the run,
     is at most `--rss-growth` (default 25%) + 16 MiB above its level right after the warm-up;
  2. descriptors and threads do not grow;
  3. the store stays bounded: with constant live data, GC must keep the stored bytes of each disk
     below `--store-factor` (default 4) × its live data + 64 MiB;
  4. every disk restores from the store to a filesystem that `xfs_repair -n` finds clean, with the
     files fsynced before the final drain intact.

Usage: sudo tests/soak/soak.py --blockgw target/release/blockgw [--minutes 30] [--disks 3]
"""
import argparse, hashlib, json, os, random, shutil, signal, statistics, subprocess, sys, threading, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "torture"))
from torture import Server, delete_device, dev_node, log  # noqa: E402


def sh(*cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)}: {(r.stdout + r.stderr).strip()[-400:]}")
    return r.stdout


def proc_stats(pid):
    with open(f"/proc/{pid}/status") as f:
        st = dict(l.split(":", 1) for l in f if ":" in l)
    return int(st["VmRSS"].split()[0]), len(os.listdir(f"/proc/{pid}/fd")), int(st["Threads"])


def held_bytes(blockgw, sock):
    """Segment bytes the worker holds by design: sealed but not uploaded, plus the open segment."""
    r = subprocess.run([blockgw, "ctl", sock, "stats", "json"], capture_output=True, text=True)
    try:
        return sum(d["queued_bytes"] + d["open_bytes"] for d in json.loads(r.stdout)["disks"])
    except (ValueError, KeyError):
        return None


def store_usage(store, prefix):
    n = b = 0
    for dirpath, _, names in os.walk(os.path.join(store, prefix, "seg")):
        for x in names:
            if not x.startswith(".tmp."):
                try:
                    s = os.path.getsize(os.path.join(dirpath, x))
                except FileNotFoundError:
                    continue
                n += 1
                b += s
    return n, b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blockgw", required=True)
    ap.add_argument("--minutes", type=float, default=30)
    ap.add_argument("--disks", type=int, default=3)
    ap.add_argument("--live-mb", type=int, default=128, help="live data per disk")
    ap.add_argument("--interval", type=float, default=10)
    ap.add_argument("--warmup", type=float, default=0.2, help="fraction of the run before the RSS baseline")
    ap.add_argument("--rss-growth", type=float, default=0.25)
    ap.add_argument("--store-factor", type=float, default=4.0)
    ap.add_argument("--restart-every", type=float, default=300, help="seconds between worker restarts (0 = never)")
    ap.add_argument("--id", type=int, default=2100)
    ap.add_argument("--seed", type=int, default=int(time.time()))
    ap.add_argument("--work", default="/tmp/blockgw-soak")
    a = ap.parse_args()
    blockgw = os.path.abspath(a.blockgw)
    work = os.path.join(a.work, str(a.seed))
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    store = os.path.join(work, "store")
    ids = [a.id + i for i in range(a.disks)]
    cfg = os.path.join(work, "soak.json")
    devs = [{"id": i, "size_gb": 2, "dir": os.path.join(work, f"img{j}"), "prefix": f"soak/d{j}", "policy": "local"} for j, i in enumerate(ids)]
    json.dump({"local_store": store, "control_socket": os.path.join(work, "soak.sock"), "devices": devs}, open(cfg, "w"))
    log(f"seed={a.seed} minutes={a.minutes} disks={a.disks} live={a.live_mb} MiB/disk work={work}")
    srv = Server(blockgw, cfg)
    mounts, failures, samples = [], [], []
    durable = {}  # path -> digest, files fsynced (contents no longer changed after)
    try:
        for i in ids:
            srv.wait_ready(i)
            m = os.path.join(work, f"mnt{i}")
            os.makedirs(m)
            sh("mkfs.xfs", "-q", "-f", dev_node(i))
            sh("mount", "-o", "noatime", dev_node(i), m)
            mounts.append(m)
        t_end = time.time() + a.minutes * 60
        stop = threading.Event()
        dlock = threading.Lock()

        def churn(k, m):
            r = random.Random(a.seed * 100 + k)
            files, n = {}, 0  # name -> size
            budget = a.live_mb << 20
            while not stop.is_set():
                op = r.random()
                live = sum(files.values())
                if op < 0.45 and live < budget:
                    size = r.choice([512, 4096, 20_000, 300_000, 2 << 20])
                    p = os.path.join(m, f"d{r.randrange(16)}", f"f{n}")
                    n += 1
                    os.makedirs(os.path.dirname(p), exist_ok=True)
                    data = r.randbytes(size)
                    with open(p, "wb") as f:
                        f.write(data)
                        if r.random() < 0.3:
                            f.flush()
                            os.fsync(f.fileno())
                            with dlock:
                                durable[p] = hashlib.sha256(data).hexdigest()
                    files[p] = size
                elif op < 0.75 and files:  # overwrite a range (the file is no longer a durable reference)
                    p = r.choice(list(files))
                    with dlock:
                        durable.pop(p, None)
                    with open(p, "r+b") as f:
                        f.seek(r.randrange(max(1, files[p])))
                        f.write(r.randbytes(r.randrange(1, 65536)))
                    files[p] = os.path.getsize(p)
                elif files:  # delete
                    p = r.choice(list(files))
                    with dlock:
                        durable.pop(p, None)
                    os.unlink(p)
                    del files[p]
                if r.random() < 0.002:
                    subprocess.run(["fstrim", m], capture_output=True)

        workers = [threading.Thread(target=churn, args=(k, m), daemon=True) for k, m in enumerate(mounts)]
        for w in workers:
            w.start()
        t0, next_restart, restarts = time.time(), time.time() + a.restart_every, 0
        while time.time() < t_end:
            time.sleep(a.interval)
            pid = srv.worker_pid()
            if not pid:
                continue
            try:
                rss, fds, thr = proc_stats(pid)
            except FileNotFoundError:
                continue
            held = held_bytes(blockgw, os.path.join(work, "soak.sock"))
            if held is None:
                continue
            rss -= held // 1024  # KiB, like VmRSS
            usage = [store_usage(store, f"soak/d{j}") for j in range(a.disks)]
            samples.append((time.time() - t0, rss, fds, thr, usage))
            if len(samples) % 6 == 0:
                log(f"t={samples[-1][0]:.0f}s rss-held={rss // 1024} MiB held={held >> 20} MiB fds={fds} threads={thr} "
                    f"store={[f'{n} segs/{b >> 20} MiB' for n, b in usage]}")
            if a.restart_every and time.time() >= next_restart:
                before = [srv.ready_count(i) for i in ids]
                os.kill(pid, signal.SIGKILL)
                for i, c in zip(ids, before):
                    srv.wait_ready(i, c + 1)
                restarts += 1
                next_restart = time.time() + a.restart_every
        stop.set()
        for w in workers:
            w.join(timeout=60)
        log(f"{len(samples)} samples, {restarts} worker restarts")

        # 1 + 2. memory, descriptors, threads (samples after the last restart are comparable)
        base_t = a.minutes * 60 * a.warmup
        after = [s for s in samples if s[0] >= base_t]
        if len(after) >= 10:
            base = statistics.median(s[1] for s in after[: max(3, len(after) // 10)])
            tail = statistics.median(s[1] for s in after[-max(3, len(after) // 5):])
            limit = base * (1 + a.rss_growth) + 16 * 1024
            msg = f"worker RSS minus held segments: {base // 1024} MiB after warm-up, {tail // 1024} MiB at the end (limit {limit // 1024:.0f} MiB)"
            (failures.append if tail > limit else log)(("memory grows: " if tail > limit else "check 1 ok: ") + msg)
            for idx, what in ((2, "file descriptors"), (3, "threads")):
                b0 = statistics.median(s[idx] for s in after[: max(3, len(after) // 10)])
                b1 = statistics.median(s[idx] for s in after[-max(3, len(after) // 5):])
                if b1 > b0 + 4:
                    failures.append(f"{what} grow: {b0} -> {b1}")
            log("check 2 done: descriptors and threads")
        else:
            failures.append("too few samples after the warm-up; run longer")

        # 3. bounded store, after a final drain
        for m in mounts:
            subprocess.run(["sync", "-f", m])
        for i in ids:
            subprocess.run([blockgw, "ctl", os.path.join(work, "soak.sock"), "drain", str(i)], capture_output=True)
            subprocess.run([blockgw, "ctl", os.path.join(work, "soak.sock"), "gc", str(i)], capture_output=True)
        for j, m in enumerate(mounts):
            live = int(sh("du", "-sb", m).split()[0])
            n, b = store_usage(store, f"soak/d{j}")
            limit = a.store_factor * live + (64 << 20)
            msg = f"disk {j}: {b >> 20} MiB in {n} segments for {live >> 20} MiB of files (limit {limit / 2**20:.0f} MiB)"
            (failures.append if b > limit else log)(("store unbounded: " if b > limit else "check 3 ok: ") + msg)

        # 4. restore every disk and check it
        for m in list(mounts):
            sh("umount", m)
            mounts.remove(m)
        for i in ids:
            subprocess.run([blockgw, "ctl", os.path.join(work, "soak.sock"), "drain", str(i)], capture_output=True)
        for i in ids:
            delete_device(blockgw, i)
        srv.proc.wait(timeout=60)
        rids = [a.id + 50 + j for j in range(a.disks)]
        rdevs = [dict(d, id=r, dir=os.path.join(work, f"rimg{j}"), hydrate_mbps=0) for j, (d, r) in enumerate(zip(devs, rids))]
        rcfg = os.path.join(work, "restore.json")
        json.dump({"local_store": store, "control_socket": os.path.join(work, "restore.sock"), "devices": rdevs}, open(rcfg, "w"))
        rs = Server(blockgw, rcfg, ["--no-supervisor"])
        try:
            for j, r in enumerate(rids):
                rs.wait_ready(r)
                rm = os.path.join(work, f"rmnt{j}")
                os.makedirs(rm)
                sh("mount", "-o", "noatime,nouuid", dev_node(r), rm)
                sh("umount", rm)
                rr = subprocess.run(["xfs_repair", "-n", dev_node(r)], capture_output=True, text=True)
                if rr.returncode != 0:
                    failures.append(f"disk {j}: restored filesystem not clean")
                    continue
                sh("mount", "-o", "ro,noatime,nouuid", dev_node(r), rm)
                src = os.path.join(work, f"mnt{ids[j]}")
                bad = checked = 0
                for p, digest in list(durable.items()):
                    if not p.startswith(src + "/"):
                        continue
                    q = os.path.join(rm, os.path.relpath(p, src))
                    try:
                        with open(q, "rb") as f:
                            ok = hashlib.sha256(f.read()).hexdigest() == digest
                    except FileNotFoundError:
                        ok = False
                    bad += not ok
                    checked += 1
                sh("umount", rm)
                if bad:
                    failures.append(f"disk {j}: {bad} of {checked} fsynced files wrong after restore")
                else:
                    log(f"check 4 ok: disk {j} restores clean, {checked} fsynced files intact")
        finally:
            for r in rids:
                delete_device(blockgw, r)
            rs.proc.wait(timeout=60)
    finally:
        for m in mounts:
            subprocess.run(["umount", "-l", m])
        for i in ids:
            delete_device(blockgw, i)
        if srv.proc.poll() is None:
            srv.proc.kill()
    with open(os.path.join(work, "samples.json"), "w") as f:
        json.dump(samples, f)
    if failures:
        for x in failures:
            log(f"FAIL: {x}")
        sys.exit(1)
    log("all checks passed")


if __name__ == "__main__":
    main()
