#!/usr/bin/env python3
"""blockgw takeover / fencing torture (needs root, ublk_drv, a built blockgw).

A single writer issues O_DIRECT 4 KiB writes (with random fsyncs) to a disk while, every few
seconds, a second server opens the same disk from the object store and takes it over, as a
replacement host would. The old server keeps running and keeps receiving writes until it notices
the takeover (split brain), its worker is killed at random meanwhile, and uploads fail at random.

Checks, at every takeover:
  1. the new owner's disk is the state after exactly the first K writes of the history so far
     (no torn block, nothing out of order);
  2. under `remote`, K covers every write the old owner acknowledged with an fsync before the new
     owner started opening the disk;
  3. the old owner is fenced: writes to it start failing with EIO, and none of the writes it took
     after the takeover appear in the new owner's history.
At the end:
  4. the last owner holds its whole history, and a fresh restore from the store equals it.

Usage: sudo tests/torture/fencetorture.py --blockgw target/release/blockgw [--policy local|remote]
                                          [--seconds 60] [--seed N] [--encrypt]
"""
import argparse, mmap, os, random, shutil, signal, subprocess, sys, threading, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from torture import BS, Server, block_bytes, delete_device, dev_node, log, parse_block, write_config  # noqa: E402

NBLOCKS = 8192  # 32 MiB working set


def read_gids(dev_id):
    fd = os.open(dev_node(dev_id), os.O_RDONLY | os.O_DIRECT)
    try:
        img = b"".join(os.pread(fd, BS * 1024, off) for off in range(0, NBLOCKS * BS, BS * 1024))
    finally:
        os.close(fd)
    return [parse_block(img[b * BS:(b + 1) * BS], b) for b in range(NBLOCKS)]


def prefix_len(history, gids):
    """K such that `gids` is exactly the state after history[:K]; raises if there is none."""
    pos = {g: i for i, (g, _) in enumerate(history)}
    held = [g for g in gids if g]
    for g in held:
        if g not in pos:
            raise AssertionError(f"write {g} is not in the history (a write from a dropped lineage or from a fenced owner)")
    k = max((pos[g] + 1 for g in held), default=0)
    expect = {}
    for g, b in history[:k]:
        expect[b] = g
    wrong = [b for b in range(NBLOCKS) if gids[b] != expect.get(b, 0)]
    if wrong:
        raise AssertionError(f"not the state after the first {k} writes ({len(wrong)} blocks differ, e.g. {wrong[:5]})")
    return k


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--blockgw", required=True)
    ap.add_argument("--policy", default="remote", choices=["local", "remote"])
    ap.add_argument("--seconds", type=float, default=60)
    ap.add_argument("--seed", type=int, default=int(time.time()))
    ap.add_argument("--id", type=int, default=1900)
    ap.add_argument("--work", default="/tmp/blockgw-fencetorture")
    ap.add_argument("--upload-error-pct", type=int, default=10)
    ap.add_argument("--encrypt", action="store_true")
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
    env = {"BLOCKGW_FAILPOINTS": f"upload=error%{a.upload_error_pct}"}
    next_id = [a.id]
    servers = []  # every server started, for cleanup: (Server, dev_id)

    def start(name):
        dev_id = next_id[0]
        next_id[0] += 1
        cfg = os.path.join(work, f"{name}.json")
        write_config(cfg, store, os.path.join(work, f"img-{name}"), dev_id, a.policy, key_file)
        s = Server(blockgw, cfg, ["--orphan-timeout", "120"], env)
        servers.append((s, dev_id))
        s.wait_ready(dev_id)
        return s, dev_id

    def stop(s, dev_id):
        for p in (s.worker_pid(), s.proc.pid):
            if p:
                try:
                    os.kill(p, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        s.proc.wait(timeout=30)
        delete_device(blockgw, dev_id)

    failures = []
    history = []  # (gid, block) of the current lineage, in write order
    gid_counter = [0]
    buf = mmap.mmap(-1, BS)

    def write(fd, block):
        gid_counter[0] += 1
        buf.seek(0)
        buf.write(block_bytes(block, gid_counter[0]))
        os.pwrite(fd, buf, block * BS)
        return gid_counter[0]

    try:
        cur, cur_id = start("s0")
        fd = os.open(dev_node(cur_id), os.O_RDWR | os.O_DIRECT)
        acked, takeovers, kills, fenced_after = 0, 0, [0], []
        t_end = time.time() + a.seconds
        n = 1
        while time.time() < t_end:
            # write phase, with the current owner's worker killed at random (it restarts on the same host)
            stop_kill = threading.Event()
            owner = cur

            def killer():
                while not stop_kill.wait(rng.uniform(0.7, 2.0)):
                    pid = owner.worker_pid()
                    if pid:
                        before = owner.ready_count(cur_id)
                        os.kill(pid, signal.SIGKILL)
                        kills[0] += 1
                        owner.wait_ready(cur_id, before + 1)

            kt = threading.Thread(target=killer, daemon=True)
            kt.start()
            phase_end = time.time() + rng.uniform(2.0, 5.0)
            while time.time() < phase_end:
                b = rng.randrange(NBLOCKS)
                history.append((write(fd, b), b))
                if rng.random() < 0.05:
                    os.fsync(fd)
                    acked = len(history)
            stop_kill.set()
            kt.join()

            # takeover: a new server opens the disk while the old one keeps running
            acked_before, issued = acked, len(history)
            new, new_id = start(f"s{n}")
            n += 1
            takeovers += 1
            # split brain: the old owner keeps taking writes until it notices it was fenced
            zombie, t0, eio = set(), time.time(), False
            while time.time() - t0 < 30:
                try:
                    b = rng.randrange(NBLOCKS)
                    zombie.add(write(fd, b))
                    if rng.random() < 0.2:
                        os.fsync(fd)
                except OSError:
                    eio = True
                    break
            if eio:
                fenced_after.append(time.time() - t0)
            else:
                failures.append(f"takeover {takeovers}: the old owner still accepted writes 30 s after the takeover")
            os.close(fd)
            stop(cur, cur_id)
            # the new owner's view
            try:
                gids = read_gids(new_id)
                leaked = [g for g in gids if g in zombie]
                if leaked:
                    raise AssertionError(f"{len(leaked)} writes the fenced owner took after the takeover are visible, e.g. {leaked[:5]}")
                k = prefix_len(history, gids)
                if k > issued:
                    raise AssertionError(f"holds {k} writes but only {issued} were issued before the takeover")
                if a.policy == "remote" and k < acked_before:
                    raise AssertionError(f"holds {k} writes but {acked_before} were acknowledged by fsync before the takeover")
                log(f"takeover {takeovers}: new owner holds the first {k} of {issued} writes (acked {acked_before}); "
                    f"old owner fenced after {fenced_after[-1] if eio else float('nan'):.2f} s, {len(zombie)} writes dropped")
                history = history[:k]
                acked = k
            except AssertionError as e:
                failures.append(f"takeover {takeovers}: {e}")
                break
            cur, cur_id = new, new_id
            fd = os.open(dev_node(cur_id), os.O_RDWR | os.O_DIRECT)

        if not failures:
            os.fsync(fd)
            os.close(fd)
            subprocess.run([blockgw, "ctl", cur.cfg.replace(".json", ".sock"), "drain", str(cur_id)], capture_output=True)
            # 4. the last owner, then a fresh restore
            try:
                k = prefix_len(history, read_gids(cur_id))
                if k != len(history):
                    raise AssertionError(f"last owner holds {k} of its {len(history)} writes")
                stop(cur, cur_id)
                fresh, fresh_id = start("fresh")
                k = prefix_len(history, read_gids(fresh_id))
                if k != len(history):
                    raise AssertionError(f"a fresh restore holds {k} of {len(history)} writes")
                log(f"final: last owner and a fresh restore both hold all {len(history)} writes")
            except AssertionError as e:
                failures.append(f"final: {e}")
        log(f"{takeovers} takeovers, {kills[0]} worker kills, {gid_counter[0]} writes issued")
    finally:
        for s, dev_id in servers:
            if s.proc.poll() is None:
                try:
                    stop(s, dev_id)
                except Exception:
                    pass
            delete_device(blockgw, dev_id)
    if failures:
        for f in failures:
            log(f"FAIL: {f}")
        log(f"reproduce with --seed {a.seed} --policy {a.policy}")
        sys.exit(1)
    log("all checks passed")


if __name__ == "__main__":
    main()
