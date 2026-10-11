# Testing

There are three layers:

| Layer | What | Needs | Run |
|---|---|---|---|
| Unit | pure logic: segment format, replay and fencing rules, write-intent tracking, in-order commit | Linux (the crate links libublk) | `cargo test` |
| End-to-end | real ublk devices, a real filesystem (XFS), a real S3 API (SeaweedFS by default) | root, `ublk_drv`, xfsprogs, python3, docker (or `S3_ENDPOINT`) | `sudo tests/e2e/run.sh` |
| Benchmark | the same workload on ext4, loop+XFS and blockgw, plus durability, multi-disk, idle, crash and restore measurements | a disposable host and a bucket | `S3_CONFIG=s3.json sudo -E bench/bench.sh` |

CI runs the unit layer and the end-to-end suite on every pull request
(`.github/workflows/ci.yml`).

## Unit tests

| Module | Tests cover |
|---|---|
| `src/log.rs` | key encoding (numeric order), segment encode/decode round trip, rejection of corrupt segments (truncation, trailing bytes, overflowing entry counts, unknown kinds), header/key mismatch, object codec (zstd vs raw for incompressible data, tombstones), applying writes and discards to an image, contiguous-prefix rule, fence targets covering gaps and the in-flight window |
| `src/track.rs` | a region needs persisting on first write only; bits clear only after the last write's segment is committed and nothing is in flight; failed writes do not pin a region; overlapping writes keep the latest segment; bitmap round trip (padding bits ignored); commit of out-of-order completions only as a contiguous prefix |

## End-to-end suite

`tests/e2e/run.sh` behaves as follows:

- It starts a SeaweedFS container unless `S3_ENDPOINT` is set, and creates the bucket.
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

Writing a case:

- The file is sourced with `tests/e2e/lib.sh` loaded. Use `write_config`, `start_server`,
  `kill9_server`, `ctl`, `stat`, `mkfs_mount`, `gen_tree`, `tree_hash`, `assert_eq` and friends.
- Exit 0 to pass, 77 (`skip`) to skip; anything else fails the case.
- Set `DISK_KEYS=a-d0` before `write_config` to address an existing disk from a second server, as a
  restore or takeover does.

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

## Not covered yet

- A systematic crash-consistency matrix: crashes injected at every step of the write, seal, upload
  and commit paths, combined with object-store faults. A torture tester in the style of lab47/lsvd's
  is planned.
- Object stores other than SeaweedFS (in CI) and AWS S3 (benchmarks). Aliyun OSS's
  `x-oss-forbid-overwrite` path is untested.
- Long-running soak tests and memory-growth tracking.
