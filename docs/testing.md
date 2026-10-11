# Testing

There are three layers:

| Layer | What | Needs | Run |
|---|---|---|---|
| Unit | pure logic: segment format, replay and fencing rules, write-intent tracking, in-order commit | Linux (the crate links libublk) | `cargo test` |
| End-to-end | real ublk devices, a real filesystem (XFS), a real S3 API (SeaweedFS by default) | root, `ublk_drv`, xfsprogs, python3, docker (or `S3_ENDPOINT`) | `sudo tests/e2e/run.sh` |
| Torture | crash consistency under random worker kills, random upload failures and store copies taken at random instants | root, `ublk_drv`, python3 | `sudo tests/torture/torture.py --blockgw target/release/blockgw --policy local` |
| Benchmark | the same workload on ext4, loop+XFS and blockgw, plus durability, multi-disk, idle, crash and restore measurements | a disposable host and a bucket | `S3_CONFIG=s3.json sudo -E bench/bench.sh` |

CI runs the unit layer and the end-to-end suite on every pull request
(`.github/workflows/ci.yml`).

- Coverage is measured for both layers with `cargo llvm-cov`. The end-to-end suite runs an
  instrumented release binary; servers that exit normally (when their devices are deleted) write
  their profiles.
- Both layers are uploaded to [Codecov](https://codecov.io/gh/dastai/blockgw) with the flags
  `unit` and `e2e`.
- Servers killed with `kill -9` in the crash cases write no profile, so recovery code is counted
  through the server that takes over.

## Unit tests

| Module | Tests cover |
|---|---|
| `src/log.rs` | key encoding (numeric order) for segments, epochs and checkpoints; tombstone detection; contiguous-prefix rule; fence targets covering gaps and the in-flight window, but not GC holes below the epoch's checkpoint; sealed = highest object is a tombstone |
| `src/segment.rs` | v3 round trip including the `committed` attestation; v2 objects still read; round trip with compressible and incompressible frames; 200 random ranged reads match the data stream; discard-only segments; detection of meta checksum, frame checksum, truncated meta or frames, and bad magic |
| `src/extmap.rs` | map follows the latest location and holes (overwrite, discard); runs; checkpoint round trip, including a disk-size mismatch and a truncated checkpoint; fetch plan groups by segment in data order |
| `src/restore.rs` | a log with a checkpoint, later segments, a tombstone and junk past it: full restore and map-only rebuild both equal a straight replay; plan without a checkpoint ends each epoch at its tombstone; a missing segment mid-epoch, a lost epoch start without a checkpoint and a checkpoint whose epoch vanished all fail the plan ("log incomplete"); foreign-data detection |
| `src/store.rs` | local-directory store: create-only PUT with exactly one winner among 16 concurrent creators, no temporary files left behind, sorted prefix listing (including a partial last component), rejection of keys that would escape the root |
| `src/metrics.rs` | Prometheus exposition: HELP/TYPE per family, one sample per disk, every sample line parses; JSON snapshot round trip |
| `src/logx.rs` | RFC 3339 UTC timestamps (including a leap day and 2099), level parsing and ordering |
| `src/failpoint.rs` | failpoint spec parsing; malformed entries ignored |
| `src/gc.rs` | deletion only of unreferenced data segments below the older checkpoint (tombstones and newer segments kept); keeping the last two checkpoints; repack candidates by live ratio, age and budget |
| `src/crypt.rs` | stored size ↔ plaintext size for chunk-boundary sizes; ranged reads at chunk boundaries, past the end, with and without a cached header, equal the plaintext; listings report plaintext sizes; stored bytes are ciphertext; a wrong key, an object moved to another key, a flipped bit, swapped chunks, truncation and a plaintext object all fail; key file formats and the permission check |
| `src/presence.rs` | presence bitmap round trip (padding bits ignored); a fetch never lands on a block being written or already written; a failed write leaves the block absent; next-absent scan across words |
| `src/track.rs` | a region needs persisting on first write only; bits clear only after the last write's segment is committed and nothing is in flight; failed writes do not pin a region; overlapping writes keep the latest segment; bitmap round trip (padding bits ignored); commit of out-of-order completions only as a contiguous prefix |

## End-to-end suite

`tests/e2e/run.sh` behaves as follows:

- It starts a SeaweedFS container unless `S3_ENDPOINT` is set, and creates the bucket.
  `E2E_STORE=dir` uses a local-directory store in each case's work dir instead. CI runs the suite
  with both stores.
- It runs each case in `tests/e2e/cases/` in its own subshell, with a scratch directory, a unique
  object prefix and a block of ublk ids.
- It always cleans up (unmount, kill servers, delete devices) and prints TAP.
- Lines starting with `# metric:` carry measurements.

| Case | Asserts |
|---|---|
| `01_basic_io` | 3,000 small files and a 64 MiB file survive unmount and remount; segments commit; queue drains; not fenced |
| `02_restore_from_s3` | a fresh "host" (new image dir, same prefix) restores a byte-identical image; files intact |
| `03_crash_odirect_write` | `kill -9` while an O_DIRECT writer has un-uploaded data: the writer sees no error, the file is intact, no kernel I/O errors, and the object store converges to the local image (verified by restoring and comparing images) |
| `04_crash_buffered_tree` | `kill -9` during a small-file copy through the page cache: the copy and `sync` complete, the tree is identical, and the object store converges |
| `05_fencing` | a second server on the same disk fences the first: `fenced=true`, raw writes fail, a `FENCED` log line appears; the new writer sees everything committed before the takeover and nothing written after it |
| `06_policy_switch` | under `remote`, the upload queue is empty whenever `fsync` returns; switching back works; `lag` updates; fsync medians are reported |
| `07_discard` | `rm` + `fstrim` releases the blocks in the local image, and discards replay on restore (byte-identical image) |
| `08_multi_disk` | four disks in one process with concurrent writers; content stays isolated; RSS reported and bounded |
| `09_idle_no_requests` | idle mounted disks issue no PUTs over 20 s |
| `10_recovery_is_bounded` | regression: after `mkfs` (a full-device discard) and an idle period, a crash leaves fewer than 4 dirty regions, not the whole disk |
| `11_supervisor_restart` | `kill -9` of the worker during an O_DIRECT write: the supervisor restarts it, the device is re-attached, the writer sees no error, and the object store converges |
| `12_orphan_timeout` | a failpoint makes every restart exit: after the orphan timeout (3 s in the test), I/O fails with EIO rather than hanging, the block device is gone, and the supervisor exits |
| `13_sigterm_drains` | with uploads slowed by a failpoint, `SIGTERM` drains the backlog (about 50 MB) before exit; a restore shows the object store holds everything |
| `14_no_supervisor_auto_attach` | without the supervisor, a plain restart after `kill -9` re-attaches the existing device; data intact, writes work |
| `15_metrics_and_ops` | `/metrics` serves every family and returns 404 for other paths; `puts_total` grows with writes; lag is 0 after a drain; `ctl metrics` matches HTTP; `stats json` parses; `check-config` accepts a valid config and rejects duplicate ids; `--version`; log lines are timestamped and leveled |
| `16_checkpoint_restore` | 20 overwrites of the same 8 MiB: drain writes a checkpoint; a restore uses it and fetches about 8 MiB of live data out of about 160 MiB uploaded; image identical |
| `17_gc` | three rounds of overwrites, each followed by `ctl gc`: GC deletes dead segments; segments left in the store = uploaded − deleted; a restore after GC is byte-identical |
| `18_lazy_restore` | a lazy restore serves the device at once with >20,000 blocks absent; a read-only mount reads the small files on demand (identical tree) while the big file stays absent; the big file is read across a worker kill; the whole device equals the source; with hydration on, nothing is left absent, the presence file goes away and the image file equals the source |
| `19_encryption` | with `key_file`, no object in the store contains a plaintext marker or a segment/checkpoint header; a fresh host with the key restores the tree; a wrong key and no key both fail to open the disk with a clear error; `check-config` refuses a world-readable key file |
| `20_missing_segment` | (dir store) a committed segment in the middle of the log is deleted: a restore fails with "log incomplete" naming it instead of serving a partial disk; with the segment back, the restore works |
| `21_lazy_crash_order` | after a lazy restore, with uploads held back by a failpoint, a write to a never-fetched block and a later write to a fetched block both survive a worker kill, and reach the store |
| `22_local_batching` | under `local`, 300 synchronous 4 KiB writes cost a handful of PUTs (1 in practice) and reach the store within the interval without a drain; with `seal_interval_ms: 0` and under `remote` every flush seals |
| `23_attach_detach` | with `devices_dir`: attach; detach refused while mounted; detach and re-attach reuses the local image; detach with `purge` and re-attach restores lazily from the store; a worker crash re-attaches from `devices_dir`; conflicting ids or prefixes are refused |
| `24_key_rotation` | a disk written under key A opens with key B plus A as an old key; B alone cannot open it, and that failed open does not fence the live writer; `rekey` rewrites the old data; B alone then restores the whole device on a fresh host |
| `25_cache_eviction` | with `cache_mb=48`, writing 160+ MiB leaves the image within the budget; the big file and a tree read back correctly after eviction, across a worker kill and after a restart |
| `26_replica` | with a standby and uploads blocked, an 8 MiB fsynced write is acknowledged (60 ms); the primary host is lost (killed, image deleted) and a new host restores the write from the standby's journal; with the standby down, fsync falls back to the object store |

Failpoints (`src/failpoint.rs`) let a case make the server fail at a named place:

- `BLOCKGW_FAILPOINTS="name=action,..."`, or a file named by `BLOCKGW_FAILPOINTS_FILE`, which every
  new worker re-reads.
- Actions are `exit`, `panic`, `error` and `sleep:<ms>`.
- The current points are `open`, hit before each disk is opened, and `upload`, hit before each
  segment PUT.

Writing a case:

- The file is sourced with `tests/e2e/lib.sh` loaded. Use `write_config`, `start_server`,
  `kill9_worker` (the supervisor restarts it), `kill9_all`, `ctl`, `stat`, `mkfs_mount`, `gen_tree`, `tree_hash`, `assert_eq` and friends.
- Exit 0 to pass, 77 (`skip`) to skip; anything else fails the case.
- Pass `DISK_KEYS=a-d0` to `write_config` to address an existing disk from a second server, as a
  restore or takeover does: `r=$(DISK_KEYS=a-d0 write_config r 1 2)`. Other per-disk settings come
  the same way (`CKPT_EVERY`, `LAZY`, `HYDRATE_MBPS`, `KEY_FILE`). Put them inside `$(...)`:
  `VAR=x r=$(write_config ...)` would set `VAR` for the rest of the case.

Running locally without root on the host, for example on macOS with OrbStack or Docker Desktop:

```sh
docker run -d --name e2e-s3 chrislusf/seaweedfs:4.47 server -s3 -dir=/data
docker run --rm --privileged --network container:e2e-s3 -v /dev:/dev -v "$PWD":/repo \
  -v "$PWD/target/release/blockgw":/usr/local/bin/blockgw:ro debian:trixie-slim bash -c \
  'apt-get update -qq && apt-get install -y -qq xfsprogs curl python3 procps util-linux bc kmod >/dev/null &&
   BLOCKGW=/usr/local/bin/blockgw S3_ENDPOINT=http://127.0.0.1:8333 /repo/tests/e2e/run.sh'
```

The VM's kernel must provide `ublk_drv` (OrbStack does). The binary must be built for Linux, for
example inside a `rust` container.

## Torture test

`tests/torture/torture.py` drives a blockgw device with a local-directory store.

- **Workload**: writer threads (`--threads`, default 1) issue O_DIRECT 4 KiB writes to random
  blocks of a 64 MiB working set. Each thread owns its own blocks and has one write in flight;
  after about 5% of its writes it calls fsync. Every block carries `(block, global write id, crc32)`,
  and the test records when each write was issued and when it completed.
- **Faults injected meanwhile**:
  - the worker is `SIGKILL`ed every 0.5–2 s; the supervisor restarts it;
  - 10% of uploads fail (`upload=error%10`) and are retried;
  - every 1–3 s the store directory is copied (with hard links: objects are immutable). A long run
    keeps a uniform sample of at most `--max-copies` (default 30) of them, so disk use does not grow
    with the duration; each is deleted once checked. Objects are only created (immutable) or deleted by
    GC, so the copy lists the checkpoints first, then copies the segments (skipping any GC deletes
    meanwhile), then only the checkpoints it listed. GC deletes checkpoints before segments, so if
    the newest listed checkpoint still exists afterwards, nothing the copy needs was deleted;
    otherwise the copy starts over. The result is a state the store really passed through.
  - checkpoints are taken every 8 segments, so GC deletes and repacks throughout (about 135 rounds
    per 60 s run).

Checks:

1. **No write is lost across worker crashes**: the device ends with the last version of every block.
2. **Every copy restores to a crash-consistent image**, with no torn block. The copy is read
   through the restored device, so lazy on-demand fetching is part of every check.
   - *Happens-before*: if the copy holds write W, it holds every write that completed before W was
     issued (or a later write to the same block). Issue times are taken before the syscall and
     completion times after it, so the check can only be too lenient, never raise a false alarm.
   - With one thread, writes are totally ordered and the check tightens to: the image equals the
     state after exactly the first K writes, for some K.
3. **Under `remote`**, the copy holds every write that completed before the start of an fsync that
   returned before the copy started.

On failure it prints the seed; rerun with `--seed N --policy P` to reproduce. `--encrypt` runs the
same test on an encrypted disk.

This test drove two design fixes. Crash-recovery re-uploads used to be split into 16 MiB segments; a
host lost halfway through that upload would leave a prefix mixing crash-time and older regions.
Recovery now uploads one atomic segment. The filesystem torture (below) exposed the second: see
"Fail-closed restore".

## Filesystem torture

`tests/torture/fstorture.py` runs XFS on a blockgw device under the same faults (worker kills,
upload failures, store copies at random instants). Four threads create files of 0 B–300 KB in
nested directories, overwrite ranges, truncate, rename and delete them; half of the created files
are fsynced together with their directory.

XFS relies on flush ordering: data that completed before a flush must be durable before anything
written after it. blockgw logs writes in arrival order and a copy restores to a prefix of the log,
so every copy must hold a consistent filesystem, under either policy.

Checks:

1. The live filesystem is clean after the run (`xfs_repair -n` after unmount).
2. Every copy restores to a filesystem that mounts (replaying its log) and that `xfs_repair -n`
   then finds clean.
3. **Under `remote`**: every file whose fsync (file and directory) returned before the copy
   started, and that was not touched again until the copy ended, is in the copy with its content.
   A typical copy verifies several hundred to a thousand such files.

**Fail-closed restore.** The first CI run of this test failed: a store copy restored to an XFS
image with a zeroed log. The copy had been taken while two checkpoints were written and GC
deleted the old ones with their segments, leaving segments but no checkpoint. Restore then
replayed the later epochs alone and served a corrupt image without complaint. Three changes came
out of it: GC deletes checkpoints before segments; opening a disk seals every older epoch (which
also closes a fencing hole after a claim that crashed midway, §3.4 of the design); and restore
refuses a log that breaks off at a gap ("log incomplete").

The checker was validated against a deliberately wrong setup: a disk configured `local` but checked
as `remote` fails check 3 within 30 s (fsynced files missing from a copy).

## Takeover torture

`tests/torture/fencetorture.py` exercises fencing the way a replacement host would trigger it. A
single writer issues O_DIRECT writes with random fsyncs; every 2–5 s a second server opens the same
disk from the store (a lazy restore) while the old one is still running. The old server keeps
receiving writes until they fail (split brain), its worker is killed at random, and 10% of uploads
fail. Write ids are globally unique, so a write from a dropped lineage is recognised.

At every takeover:

1. The new owner's disk is the state after exactly the first K writes of the history so far.
2. Under `remote`, K covers every write the old owner acknowledged with an fsync before the
   takeover began.
3. The old owner is fenced: its writes start failing with EIO (typically within milliseconds of
   its next upload), and none of the writes it took after the takeover appear in the new owner.

At the end, the last owner holds its whole history and a fresh restore equals it. A 45 s run makes
10–13 takeovers.

With `--replica`, a standby runs, and every takeover loses the old host instead (supervisor and
worker killed, local image deleted): the new owner must hold every write acknowledged by fsync,
under `local` too. Wiping the standby's journal before each takeover makes this fail within the
first takeover.

Its first run found a lazy-restore bug: after a worker crash, recovery re-uploaded only blocks the
(stale) presence bitmap marked present, dropping writes to blocks that had never been fetched while
keeping later ones. See §3.5b of the design; e2e case `21_lazy_crash_order` reproduces it
deterministically.

## Soak test

`tests/soak/soak.py` runs XFS on three disks for a long time (45 minutes in the scheduled job)
while worker threads overwrite, append to and delete files within a fixed amount of live data
(128 MiB per disk), with fsyncs and occasional `fstrim`; the worker is killed every 5 minutes. It
samples worker RSS, open descriptors, threads and the store's size per disk.

Checks:

1. **Memory**: worker RSS, minus the segment bytes it holds by design (queued for upload and the
   open segment), ends at most 25% + 16 MiB above its level after the warm-up.
2. **Descriptors and threads** do not grow.
3. **Bounded store**: with constant live data, GC keeps each disk's stored bytes below 4× its
   live data + 64 MiB.
4. Every disk restores to a filesystem that `xfs_repair -n` finds clean, with its fsynced files
   intact.

Findings so far:

- The first run measured 110–140 MiB of worker RSS (beyond held segments) for three disks: glibc's
  per-thread malloc arenas keeping freed segment buffers. blockgw now limits glibc to two arenas
  at startup, and the same run measured 50–56 MiB.
- After batched sealing (large segments), the first daily run found the store at 12–25× the live
  data: checkpoints were only written per 64 segments, and GC keeps two checkpoint intervals.
  Checkpoints are now also written per 64 MiB written (`checkpoint_mb`); a 10-minute run holds
  2–3× the live data. With larger segments the worker holds more in flight: 90–125 MiB of RSS
  beyond queued segments for three disks writing tens of MB/s.
- That run also saw one EIO on a log write that blockgw did not explain. It has not recurred;
  every failed I/O is now logged with its cause and errno.

## In CI

Each pull request runs the torture tests for 90 s each, with the run id as seed, so every run
explores a different schedule. A daily workflow (`soak.yml`, also started by hand) runs the soak
test for 45 minutes and the 4-writer, XFS and takeover tortures for 20 minutes each.

| Job | Script and options |
|---|---|
| local policy | `torture.py --policy local` |
| remote policy | `torture.py --policy remote` |
| remote, 4 writers | `torture.py --policy remote --threads 4` |
| remote, encrypted | `torture.py --policy remote --encrypt` |
| XFS, local policy | `fstorture.py --policy local` |
| XFS, remote policy | `fstorture.py --policy remote` |
| remote, 4 writers, 16 MiB cache | `torture.py --policy remote --threads 4 --cache-mb 16` |
| XFS, local policy, 32 MiB cache | `fstorture.py --policy local --cache-mb 32` |
| takeovers, local policy | `fencetorture.py --policy local` |
| takeovers, remote policy | `fencetorture.py --policy remote` |
| host loss with a standby, local policy | `fencetorture.py --policy local --replica` |

## Benchmark

`bench/bench.sh` creates and deletes ublk and loop devices; run it only on a disposable host. Its
phases are:

| Phase | Measures |
|---|---|
| `KERNEL` | kernel version, ublk availability |
| `SINGLE` | clone, status, rg, tar, fsync, rm, and fio (4K random, 1M sequential) on ext4, loop+XFS and blockgw |
| `POLICY` | fsync under `local` and `remote`, a live switch during an fsync loop, clone under `remote` |
| `MULTI` | N disks (default 16) cloning concurrently, against N loop devices; RSS |
| `IDLE` | PUTs from idle mounted disks |
| `KILL` | `kill -9` during an O_DIRECT write and during a clone |
| `RESTORE` | a fresh-host restore and image hash; fencing of the old writer |

Results land in `$BENCH_DIR/results/bench.tsv`. The October 2026 numbers are in
[results/2026-10-prototype.md](results/2026-10-prototype.md).

## Real object stores

CI runs the e2e suite against SeaweedFS. On demand it also runs against a real store: set
`S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY` and `S3_SECRET_KEY`.

- **Cloudflare R2** (October 2026, from a host far from the region): all 22 cases pass
  (`20_missing_segment` is dir-store only). Create-only PUTs, ranged reads, encryption, lazy
  restore, fencing and missing-object detection behave as on SeaweedFS. Two cases first failed
  there and led to changes: recovery no longer waits for its upload before re-attaching (the
  worker took longer than the orphan timeout), and `22_local_batching` counts sealed segments
  after a drain rather than completed PUTs.
- **AWS S3**: the prototype benchmarks (`docs/results/2026-10-prototype.md`).

## Not covered yet

- xfstests / fsstress on top of blockgw (the filesystem torture uses its own workload; Debian and
  Ubuntu do not package fsstress).
- Object stores other than SeaweedFS (in CI), Cloudflare R2 (e2e on demand) and AWS S3
  (benchmarks). Aliyun OSS's `x-oss-forbid-overwrite` path is untested.
