# Comparison with alternatives

This page compares blockgw with the other ways to give a host persistent, movable, object-storage-
backed disks. Where we measured something ourselves, the numbers come from the same workload: a
shallow `git clone` of microsoft/TypeScript (about 67k files), then `git status`, `rg`, `tar` and
`rm -rf`. Measurement conditions differ between rows, so each table says where its numbers come
from. Claims about other projects that we did not measure come from their documentation, which is
linked.

> We wrote blockgw, and these measurements were made to choose a design, not as a neutral
> benchmark. Each project has workloads where it is the better choice; the "When it is the better
> choice" column tries to say which.

## Summary matrix

| | Unit | Hot path | `fsync` reaches object storage | Idle cost | Fencing of an old writer | Multi-host sharing | License |
|---|---|---|---|---|---|---|---|
| **blockgw** | block (ublk) | local image, in-kernel block I/O | per-disk policy: `local` (~1–3 ms) or `remote` (~59 ms) | no requests | epoch plus create-only tombstones | no (single writer) | MIT / Apache-2.0 |
| [ZeroFS](https://github.com/Barre/ZeroFS) (NBD mode) | block (NBD) and files (NFS / 9P) | gateway memory, local cache, object store | always (~245 ms per fsync on S3) | ~6,000 GETs per hour per gateway process (measured) | writer epoch in SlateDB | files: yes, via NFS / 9P; block: single writer | AGPL-3.0 |
| [JuiceFS](https://github.com/juicedata/juicefs) | files (FUSE) | metadata DB plus object store per file operation | on close or fsync | metadata DB | n/a | yes | Apache-2.0 |
| [Archil](https://archil.com) | files (managed) | managed service | managed | managed | n/a | yes | proprietary |
| [s3backer](https://github.com/archiecobbs/s3backer) | block (FUSE / NBD) | block cache; one object per block | per block written | none | none ([README](https://github.com/archiecobbs/s3backer)) | no | GPL-2.0 |
| [nbdkit S3 plugin](https://libguestfs.org/nbdkit-S3-plugin.1.html) | block (NBD) | one object per block; read-modify-write | per block written | none | none | no | BSD-style |
| [LSVD](https://par.nsf.gov/biblio/10383219-beating-bottleneck-case-log-structured-virtual-disks) ([lsvd-rbd](https://github.com/CCI-MOC/lsvd-rbd), [lab47/lsvd](https://github.com/lab47/lsvd)) | block | local SSD log and cache | log shipped in order | — | lease (lab47) | no | GPL-2.0 / Apache-2.0 |
| [ubiblk](https://github.com/ubicloud/ubiblk) | block (vhost-user-blk, ublk) | local disk | archive and base image only ([archive.md](https://github.com/ubicloud/ubiblk/blob/main/docs/archive.md)) | — | — | no | AGPL-3.0 |
| Cloud volume per workspace (e.g. EBS gp3) | block | network block storage | always (replicated volume) | pay for provisioned GB | attach / detach | no | — |
| Ceph RBD / Longhorn | block | replicated over the network | always (replicated) | cluster running | exclusive lock | no | LGPL / Apache-2.0 |

## Measured: file systems over object storage

These numbers come from a local 4-vCPU VM with a loopback MinIO, plus the real Archil service, all
measured in October 2026. They are ratios to ext4 on the same host.

| Approach | `git clone` | hot `git status` | `rm -rf` |
|---|---:|---:|---:|
| JuiceFS, defaults (PostgreSQL metadata) | 448× | 72× | 582× |
| JuiceFS, tuned (Redis, 300 s cache, writeback) | 52× | 4.2× | 19× |
| ZeroFS over 9P (FUSE client) | 16× | 34× | 62× |
| ZeroFS over the kernel v9fs client | 8.3× | 6.8× | 17× |
| ZeroFS over NFS | 53× | — | — |
| Archil (managed) | 51× | 424× | — |
| **ZeroFS NBD + XFS** (block device) | **1.0×** | **0.8×** | 1–2.3× |

- File-level designs pay several metadata or network round trips per file operation. That is fine
  for shared data and large files, but not for source trees and package-manager directories.
- Putting a local filesystem on a virtual block device removes the per-file cost. This is the
  approach blockgw takes as well.

## ZeroFS

ZeroFS is the closest design: an object-store-backed block device (NBD) whose storage engine is the
SlateDB LSM. It is also a filesystem server (NFS / 9P) with multi-client access, which blockgw is
not. We measured ZeroFS v2.3.5 in NBD mode in October 2026: first locally, then on Hetzner (hil)
with AWS S3 us-west-2 about 9 ms away.

### Single disk on real S3

ZeroFS ran on a 4-vCPU host and blockgw on a 3-vCPU host; ext4 is each host's own root filesystem.

| | ext4 | ZeroFS NBD | blockgw (`local`) |
|---|---:|---:|---:|
| `git clone` of 67k files | 3.84–3.89 s | 5.53 s | 4.63–5.11 s |
| fsync of 4 KiB, median | 0.8 ms | 240 ms | 1.6–3.1 ms (`remote`: 59 ms) |
| cold `git status` | 0.2–2.1 s | 1.06 s | 0.56 s |
| cold `rg` | 5.6–6.1 s | 11.6 s | 5.1 s |
| cold `tar` | 15.8–18.4 s | 20.7 s | 13.1–16.5 s |

### Many disks on one gateway

- Every NBD FLUSH in a ZeroFS process seals the process-wide segment and flushes the whole database
  ([`fs/flush_coordinator.rs`](https://github.com/Barre/ZeroFS/blob/v2.3.5/zerofs/src/fs/flush_coordinator.rs)).
  Disks on one gateway therefore wait for each other's data. We measured this with 16 disks
  continuously writing (clone, edit, commit, delete):
  - one gateway process: p90 59.7 s per iteration, against 15.2 s on ext4;
  - the same disks split over four processes on the same host: p90 13.9 s.
- blockgw has a flush domain per disk. With 16 disks writing concurrently on a 3-vCPU host, its
  per-disk median (15.2 s) matched kernel loop devices (15.8 s).

### Per-process cost

| | ZeroFS (per gateway process) | blockgw (one process, 16 disks) |
|---|---|---|
| Resident memory, idle | about 53 MB of anonymous memory, whatever the cache size | 15.5 MB in total |
| Object-store requests, idle | about 5,800–6,300 per hour, almost all GETs (SlateDB manifest and compactor polling) | 0 |
| Write stall | about 12 s every ~256 consecutive fsyncs (SlateDB L0 backpressure at `l0_max_ssts = 256`; lowering the limit made it worse) | none observed |

### Durability and failover

| | ZeroFS | blockgw |
|---|---|---|
| Gateway crash | unflushed acknowledged writes are lost on a single gateway | `local`: the image is on local disk, so nothing is lost if the host survives (the restarted worker re-uploads the tail); `remote`: nothing acknowledged is lost |
| Standby | Connected HA keeps unflushed writes alive on a standby: all 5,617–6,693 acknowledged records survived a leader kill in our tests, with the standby serving 6–7 s later | `replica`: a standby journal keeps acknowledged writes; the takeover torture with host loss checks that every fsync-acknowledged write survives |
| Single writer | writer fencing is built in: a second instance on the same prefix fences the first within about 1 s | writer fencing is built in too (epochs plus tombstones) |

### Where ZeroFS is the better choice

- You need file-level sharing (NFS / 9P).
- Hosts cannot hold a full copy of each disk: ZeroFS keeps data in S3 with a bounded cache. blockgw
  restores lazily, but hydrates to the full image and has no eviction yet.
- You need file-level access from several clients at once.

## Block-per-object designs (s3backer, nbdkit S3)

These store each fixed-size block as its own object.

- Writes smaller than a block need a read-modify-write.
- Objects are overwritten in place, so a flush has no atomic commit point.
- There is no protection against two writers.

They suit read-mostly images or backups, not busy filesystems. We did not measure them.

## Log-structured virtual disks (LSVD, lab47/lsvd)

The LSVD paper (EuroSys 2022) argues for the same core idea: a local log and cache that is shipped,
in order, to a remote store, so that any prefix is a consistent state.

- The authors' implementation, lsvd-rbd, uses Ceph RADOS as its backend and says it is not
  production-ready.
- lab47/lsvd (Go, Apache-2.0) is close to blockgw's design: segments, an extent map, NBD, S3 and
  leases. It is labelled alpha.

## Local-disk block devices with object-storage archives (ubiblk)

- ubiblk serves VM disks from local storage (vhost-user-blk, with ublk marked experimental).
- It uses object storage for base images and archives, not as a continuously shipped log.

## Cloud volumes and distributed block storage

- **A cloud volume per workspace** is simple and durable, but:
  - you pay for provisioned capacity while idle;
  - instances have attachment limits, often 28 shared with network interfaces on Nitro instances
    ([EBS volume limits](https://docs.aws.amazon.com/ebs/latest/userguide/volume_limits.html)).
- **Ceph RBD or Longhorn** replicate uncompressed blocks three times and need a cluster to operate.
  Object storage stores compressed segments with the provider's durability.

## Object-store mounts (Mountpoint for S3, s3fs, goofys)

These map objects to files, with limited POSIX semantics. For example, Mountpoint does not support
renames or modifying existing files
([SEMANTICS.md](https://github.com/awslabs/mountpoint-s3/blob/main/doc/SEMANTICS.md)). Source trees
and package managers need full POSIX, so these are not candidates for workspaces.
