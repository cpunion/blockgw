# Prototype results, October 2026

## Setup

| Item | Details |
|---|---|
| Host | Hetzner Cloud cpx21 (3 vCPU, 3.8 GB RAM, local SSD), Hillsboro (OR) |
| OS and kernel | Debian 13, kernel `6.12.107+deb13-cloud-amd64` (`ublk_drv` loads, `/dev/ublk-control` present) |
| Object store | AWS S3 us-west-2, about 9 ms TLS round trip from the host |
| blockgw | commit of this repository's initial import; `MALLOC_ARENA_MAX=2`; `local` policy unless noted; default `max_lag` 16 MiB |
| Baselines | `ext4` is a directory on the root filesystem. `loop` is XFS on a kernel loop device over a sparse file on the same disk; it goes through the host page cache, so its IOPS and cold-read numbers are inflated |
| Workload | shallow clone of microsoft/TypeScript (67,291 files) from a local bare mirror, then `git status`, `rg -c TODO`, `tar cf - \| wc -c`, 4 KiB fsync ×50, `rm -rf`, and fio with io_uring and O_DIRECT |

Single runs on one host: treat differences under about 15% as noise.

## Single disk

| Metric | ext4 | loop + XFS | blockgw |
|---|---:|---:|---:|
| clone of 67k files | 3.89 s | 4.50 s | 4.63–5.11 s |
| sync after clone | 0.67 s | 1.37 s | 2.06–2.16 s |
| hot `git status` | 115 ms | 89 ms | 79–92 ms |
| cold `git status` | 277 ms | 636 ms | 562–572 ms |
| cold `rg` | 5.6 s | 9.0 s | 5.1 s |
| hot `rg` | 243 ms | 170 ms | 160–224 ms |
| cold `tar` | 15.8 s | 3.8 s* | 13.1–16.5 s |
| fsync of 4 KiB, median / p90 | 0.8 / 1.1 ms | 2.3 / 3.1 ms | 1.6–3.1 / 2.0–3.8 ms |
| `rm -rf` + sync | 0.79 s | 2.14 s | 2.17–3.12 s |
| 4K random write IOPS | 48.6k | 130k* | 31.7–33.8k |
| 4K random read IOPS | 53.9k | 297k* | 53.1–54.2k |
| 1 MiB sequential write | 3,235 MB/s | 2,708 MB/s | 57 → 197–207 MB/s (see below) |
| 1 MiB sequential read | 4,104 MB/s | 1,396 MB/s* | 2,921–3,225 MB/s |

\* inflated by the host page cache under the loop device.

- **Small files**: the small-file workload runs within 1.3× of ext4. Cold reads match ext4 because
  the data is local.
- **Sequential writes** are bounded by upload bandwidth once the `local` lag limit is reached. The
  first version uploaded one segment at a time and reached 57 MB/s. Uploading up to 8 segments
  concurrently per disk (with the fencing window extended to match) raised that to about 200 MB/s.
- **Random writes** run at about 70% of ext4. Each write is also copied into the segment buffer,
  and the first write into a clean 4 MiB region waits for the intent bit to be persisted.

## Durability policy

| Metric | Value |
|---|---|
| fsync median, `local` | 0.8 ms |
| fsync median, `remote` | 59.0 ms (p90 66.7 ms). The first fsync after switching took 2.7 s because it waited for the backlog |
| clone + sync, `remote` | 7.53 s |
| live switch during an fsync loop | `local` 1.3 ms → `remote` 58.5 ms → `local` 1.3 ms; no remount |

## One process, 16 disks

| Metric | Value |
|---|---|
| Concurrent clone of a 5,302-file repository on each disk | per-disk median 15.2 s, against 15.8 s on 16 loop devices (CPU-bound on 3 vCPUs) |
| RSS, idle | 15.5 MB (66 threads) |
| RSS after the clones / after drain | 28.8 MB / 43.7 MB |
| RSS after a 5.5 GB write burst on one disk | 23.8 MB with `MALLOC_ARENA_MAX=2`; 550 MB without it (glibc per-thread arenas of the 16 upload workers) |
| PUTs from 16 idle mounted disks over 120 s | 0. No GET or LIST is issued after open |

## Crash, restore and fencing

| Scenario | Result |
|---|---|
| `kill -9` during a 500 MB O_DIRECT write, 64 MiB not yet uploaded | Recovery re-uploaded 14 regions (54 MiB) in 361 ms and the disk was serving again after 960 ms. dd exit status 0, file byte-identical, 0 kernel I/O errors, no XFS shutdown |
| `kill -9` during a clone | clone exit status 0, `git fsck` clean, 0 kernel errors. Locally, re-attach took 39–56 ms |
| Restore on a fresh host (new image dir, same prefix) | 3.8 s for 125 segments across several epochs, including tombstones. Image sha256 identical; `git fsck` clean |
| Old writer after the takeover | its next upload collided and it was fenced; further I/O returned EIO |

## Bugs the prototype run found (all fixed before this import)

| Symptom | Cause and fix |
|---|---|
| After `mkfs` (full-device discard) under continuous load, a crash marked all 5,120 regions dirty, and recovery read 20 GB into memory | Intent bits were only cleared when the disk went fully idle. They are now cleared per region once its last segment commits; recovery reads only data extents and applies backpressure |
| Restore failed on ext4 with EINVAL | Segment payloads were written through the O_DIRECT image handle, and they are not aligned. Replay now uses a buffered handle |
| Restore took 309 s | Segments were fetched one at a time. They are now fetched in parallel batches of 16 (309 s → 2.9–3.8 s) |
| `blockgw del` deadlocked | It sent DEL while the server still held the device. It now sends STOP first |
| 550 MB RSS after a burst | Per-thread malloc arenas. Fixed with `MALLOC_ARENA_MAX=2`, plus `malloc_trim` when a disk's queue drains |

## Open issues found

- A device whose server died and was never recovered queues I/O forever; even `umount` blocks.
  This needs a supervisor timeout that deletes the device (roadmap item 1).
- Re-upload finishes before the device is re-attached, so the stall grows with the un-uploaded
  backlog: about 1 s for 54 MiB.

## Comparison numbers

For ZeroFS and the file-level designs, see [comparison.md](../comparison.md).
