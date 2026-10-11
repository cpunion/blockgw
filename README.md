# blockgw

[![CI](https://github.com/dastai/blockgw/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/dastai/blockgw/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/dastai/blockgw/graph/badge.svg)](https://codecov.io/gh/dastai/blockgw)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

Virtual block devices for Linux that run at local-disk speed and persist to S3-compatible object
storage, or to a local directory.

One `blockgw` process per host serves all of that host's virtual disks through
[ublk](https://docs.kernel.org/block/ublk.html), the io_uring-based userspace block driver. Each
disk keeps its full current image in a local sparse file, so reads and writes go to local disk.
Its history is a strictly ordered log of segments uploaded to S3, R2, or any S3-compatible store.
You can restore a disk on any host from the log alone, and a second host that opens the disk fences
out the first.

> **Status: experimental prototype.** The write path, crash recovery and supervision, checkpointed
> restore, garbage collection and fencing work and are covered by tests. Cold reads that fetch data
> lazily from S3, and encryption, are not implemented yet. Do not use it for data you cannot afford to lose. See
> [Roadmap](#roadmap).

## Why

Workloads such as AI-agent sandboxes, cloud dev environments and CI workspaces share a profile:

- many workspaces, most of them idle at any moment;
- each one runs small-file-heavy tools (git, package managers, compilers, `rg`);
- a workspace must outlive its host and move between hosts.

The usual options each miss one of these needs:

| Option | What goes wrong |
|---|---|
| File systems over object storage (FUSE, 9P, NFS gateways) | Every file operation pays a userspace or network round trip. In our measurements a `git clone` of a 67k-file repository ran 16–450× slower than on ext4 ([comparison](docs/comparison.md)). |
| A cloud block volume per workspace | You pay for provisioned capacity while idle, and instances have per-instance attachment limits. |
| Self-run distributed block storage (Ceph RBD, Longhorn) | Three replicas of uncompressed data plus operational weight. |
| ZeroFS in block (NBD) mode, the closest existing design | Every disk on a gateway shares one LSM and one flush domain. An fsync waits for an object-store round trip, and an idle gateway keeps polling the store ([comparison](docs/comparison.md#zerofs)). |

blockgw keeps the hot path on local disk inside the kernel's block layer. It makes object storage
the durable, mobile copy, and it lets each disk choose when an `fsync` must reach that copy.

## How it works

```
 workload (any filesystem; XFS in our tests)
   │ syscalls
 kernel: page cache → XFS → /dev/ublkbN            one ublk device per disk, multi-queue
   │ io_uring passthrough commands
 blockgw (one process per host)
   ├─ per disk: sparse image file (O_DIRECT, io_uring), write-intent region map, open segment, epoch
   └─ shared upload pool: segments → create-only PUTs, ≤8 in flight per disk, committed in order
 object storage:  <prefix>/epoch/<e>   <prefix>/seg/<e>/<seq>
```

- **Write**: mark the region's intent bit (made durable before the first write into a clean 4 MiB
  region), write the image, then copy the data into the disk's open segment.
- **Flush**: `fdatasync` the image, then seal the open segment, which goes to the upload pool.
  What happens next depends on the disk's durability policy:
  - `local`: the flush returns once the local `fdatasync` completes. Uploads lag by at most
    `max_lag` (default 16 MiB); past that, writers wait for uploads to catch up.
  - `remote`: the flush returns only once every segment sealed so far is in the object store.

  You can switch the policy per disk at runtime (`blockgw ctl SOCK policy <id> local|remote`).
- **Crash-consistent object store**: segments are cut at flush boundaries and committed in sequence
  order. Any prefix of the log is therefore a state the disk could have crashed into. A new host
  replays the prefix up to the first gap or tombstone.
- **Checkpoints and ranged reads**: segments are stored as independently compressed 256 KiB frames
  behind a small index. A committed extent map (block → segment and offset) is checkpointed every 64
  segments.
  - A restore loads the newest checkpoint, fetches only the live data with ranged GETs, then replays
    the segments after it.
  - After heavy overwriting it downloads what is current, not the history.
- **Garbage collection**: after each checkpoint, segments that neither of the last two checkpoints
  references are deleted, and sparse segments are repacked. Storage tracks live data, not
  history. Tombstones are kept forever, so a fenced writer always collides.
- **Crash recovery**: `blockgw serve` is a small supervisor around a worker process.
  - If the worker dies, the kernel keeps the device and queues its I/O (`UBLK_F_USER_RECOVERY`).
    The supervisor restarts the worker at once, and the new worker re-attaches the device.
  - If no worker comes back within `--orphan-timeout` (30 s by default), the supervisor stops the
    devices, so I/O fails with `EIO` instead of hanging forever. On reopen, every region
  whose intent bit is still set is re-uploaded before the device is served again (holes are skipped
  using `SEEK_DATA`), so the object store converges to the local image.
- **Fencing**: opening a disk claims epoch `e+1` with a create-only PUT. It then writes tombstones
  into the previous epoch's free sequence numbers: every gap, plus a window past its highest
  segment. The old writer's next upload collides with a tombstone (HTTP 412), and the old writer
  fails all further I/O.
- **Idle cost**: none. After a disk is open, blockgw makes no object-store requests unless it has
  segments to upload or a disk to restore.

The full contract is in [docs/design.md](docs/design.md).

## Results (prototype, October 2026)

Test setup:

- Hetzner cpx21 (3 vCPU, 4 GB), Debian 13 with kernel 6.12;
- AWS S3 in us-west-2, about 9 ms away from the host;
- ext4 is the host's root filesystem.

Full details are in [docs/results/2026-10-prototype.md](docs/results/2026-10-prototype.md).

| | ext4 | blockgw (`local`) |
|---|---:|---:|
| `git clone` of 67k files | 3.89 s | 4.63–5.11 s |
| cold `rg` / cold `tar` | 5.6 s / 15.8 s | 5.1 s / 13.1–16.5 s |
| fsync of 4 KiB, median | 0.8 ms | 1.6–3.1 ms (`remote` policy: 59 ms) |
| 4K random write / read IOPS | 48.6k / 53.9k | 31.7–33.8k / 53.1–54.2k |
| 1 MiB sequential write / read | 3,235 / 4,104 MB/s | ~200 / 3,225 MB/s (writes are bounded by upload bandwidth) |

- **Memory**: one process serving 16 idle disks used 15.5 MB RSS, and 24–44 MB after load.
- **Idle requests**: 0 object-store requests while idle.
- **kill -9 under load**: the workload saw no errors, the tail was re-uploaded, and the process was
  serving again in under 1 s.
- **Restore on a fresh host**: the restored image was byte-identical (3.8 s for 125 segments).

## Quick start

Requirements:

- Linux 6.0 or newer with the `ublk_drv` module. Debian 13 ships it; on Ubuntu, install
  `linux-modules-extra-$(uname -r)`.
- Root.
- An S3-compatible bucket with conditional writes (`If-None-Match: *`). AWS S3, Cloudflare R2,
  SeaweedFS and MinIO support them. For development and tests, or with a second disk or NAS mount as
  the durable copy, a local directory works too (`"local_store": "/path"`, see
  [examples/config.local.json](examples/config.local.json)).
- To build: Rust 1.85 or newer, plus `clang` and `libclang-dev`, which libublk's bindgen step needs.

```sh
cargo build --release
sudo modprobe ublk_drv
cp examples/config.json /etc/blockgw.json   # edit endpoint, bucket, credentials, disks (or use config.local.json)
sudo MALLOC_ARENA_MAX=2 target/release/blockgw serve /etc/blockgw.json &   # supervisor + worker
sudo mkfs.xfs /dev/ublkb0 && sudo mount /dev/ublkb0 /mnt

target/release/blockgw ctl /run/blockgw.sock stats
target/release/blockgw ctl /run/blockgw.sock policy 0 remote   # fsync now waits for S3
target/release/blockgw ctl /run/blockgw.sock drain 0           # upload everything sealed so far
target/release/blockgw ctl /run/blockgw.sock metrics           # Prometheus text (also at metrics_addr)
```

Running the service (a systemd unit is in [examples/blockgw.service](examples/blockgw.service); metrics,
alerts and the control socket are in [docs/operations.md](docs/operations.md)):

- **Restarts**: the supervisor restarts a crashed worker by itself. A fresh `blockgw serve` after the
  whole service died also re-attaches devices that are waiting for a server, and adds any that are
  missing; no flag is needed.
- **Shutdown**: `SIGTERM` drains every disk's upload queue, then exits, and leaves the devices
  recoverable by the next start.
- **Moving a disk**: unmount it, run `drain`, stop the service, then start a server with the same
  `prefix` on the new host. The image is restored from the object store.

On macOS or Windows, run blockgw in a Linux VM; see [docs/local-dev.md](docs/local-dev.md).

## Testing

- `cargo test`: unit tests for the segment format, replay and fencing rules, write-intent tracking
  and in-order commit.
- `sudo tests/e2e/run.sh`: seventeen end-to-end cases against real ublk devices, with a local
  SeaweedFS or (`E2E_STORE=dir`) a local-directory store (crash recovery, supervisor restart, orphan
  timeout, graceful drain, restore, fencing, policy switch, discard, multi-disk, idle requests,
  metrics, checkpoint restore, garbage collection, lazy restore, ...).
- `sudo tests/torture/torture.py`: crash-consistency torture. Random worker kills, random upload
  failures and store copies taken at random instants; every copy must restore to an exact prefix
  of the writes, with nothing lost that `remote` acknowledged.
- `bench/bench.sh`: the benchmark behind the numbers above.

See [docs/testing.md](docs/testing.md).

## Roadmap

1. Encryption at rest (per-disk keys, per frame).
2. Optional streaming of unflushed writes to a standby.
3. Bounded local cache: evict cold, already-uploaded blocks so a host keeps only a working set
   (lazy restore already fetches blocks on first read).
4. Zero-copy ublk (Linux 6.15+); concurrent-writer and filesystem-level (fsstress) torture.
5. Native macOS and Windows frontends (research: an iSCSI target for Windows' built-in initiator;
   an FSKit-served image file attached with `hdiutil` on macOS).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Changes come in as pull requests from forks.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
