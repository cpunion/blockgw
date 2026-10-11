# blockgw design

This document is the contract: what a blockgw disk guarantees, how the object-store log is laid
out, how writers are fenced, and how failures surface. Where the prototype does not yet implement
part of the contract, the text says so.

## 1. Goals and non-goals

Goals:

1. **One server process per host** serves every virtual disk on that host. Each disk is used by one
   consumer, typically one sandbox or VM; several consumers on the same host may share a mounted
   disk.
2. **Use the kernel**:
   - ublk with io_uring passthrough on the device side;
   - io_uring with O_DIRECT for the local image;
   - the page cache for hot reads;
   - discard passed through to the image and to the log.
3. **A process per disk stays an option**: the same binary can serve a single disk when stronger
   isolation is needed.
4. **Good performance and cost for both many small files and large files.**
   - Small files: performance stays close to the local disk, and object-store requests scale with
     bytes and flushes, not with file count.
   - Large files: throughput is bounded by upload bandwidth, not by request overhead.
5. **No cost while idle**: no polling of the object store.

Non-goals:

- Multi-host read-write sharing of one disk. A disk carries an ordinary single-host filesystem;
  shared file access belongs to a network filesystem.
- Replacing the filesystem. blockgw is a block device, and any Linux filesystem can sit on it.

## 2. Kernel facilities

The table was checked against the Debian 13 (6.12) kernel configuration in October 2026.

| Facility | Status | Use |
|---|---|---|
| ublk (`CONFIG_BLK_DEV_UBLK=m`) | available | userspace block device on io_uring; multi-queue. `UBLK_F_USER_RECOVERY` keeps the device while the server restarts |
| ublk zero copy (`UBLK_F_SUPPORT_ZERO_COPY`, `UBLK_F_AUTO_BUF_REG`) | needs Linux 6.15 / 6.16 | removes one memcpy per I/O; not used yet |
| io_uring | available | image I/O; waiting on cross-thread events (eventfd reads on the queue ring) |
| dm-clone | **not built** in Debian kernels | would allow lazy hydration of a local device from a remote one |
| dm-cache / dm-writecache | available | not used, see §7 |
| DRBD | available | alternative for synchronous replication, see §7 |
| XFS reflink, discard | available | used by the filesystem on top as usual |

The design keeps ordering and durability decisions in userspace, where the log is written, rather
than stacking device-mapper targets whose write-back order is unrelated to filesystem flushes.

## 3. Contract

### 3.0 Object stores

The log needs three operations from its store:

- a **create-only PUT**, which fails if the key exists; fencing rests on it;
- **GET**;
- **LIST** of a prefix in lexicographic order.

Two stores implement them:

| Store | Create-only | Durability |
|---|---|---|
| S3-compatible (`"s3"`) | `If-None-Match: *`. A 412 or 409 response means the key exists | the provider's |
| Local directory (`"local_store"`) | write a temporary file, fsync it, then `link(2)` it to the key's path; EEXIST means the key exists | the directory's filesystem; the parent directory is fsync'd after each new key |

A local directory serves development and tests, or a second disk or NAS mount as the durable copy.
Two hosts can fence each other through it only if they share the directory. On NFS, `link` is the
classic exclusive-create primitive, but that path is untested.

### 3.1 Disk identity and ownership

- A disk is an object prefix (`<prefix>/`), a size, and a sequence of epochs.
- **At most one host may have a disk open read-write at any time.** The platform's scheduler or
  lease enforces this; blockgw's epoch fencing (§3.4) is the safety net when it fails.
- On one host, several consumers may share a disk by bind-mounting the same filesystem.
- Read-only access from another host is possible by restoring from the log. Opening a disk
  read-only without fencing is not implemented.

### 3.2 Durability policies (per disk, switchable at runtime)

| Policy | Where data is when `fsync` returns | `fsync` latency (measured) | Loss if the host is lost |
|---|---|---|---|
| `local` | on the host's disk (image `fdatasync`) | 0.8–3 ms | the un-uploaded tail, bounded by `max_lag` |
| `remote` | in the object store | about 59 ms (host about 9 ms from S3) | nothing acknowledged by `fsync` |

Under both policies:

- **The object store always holds a crash-consistent prefix.** Segments are cut at flush
  boundaries, or early at 16 MiB, which is also a valid crash point. They are committed in
  sequence order, so any committed prefix is a state the disk could have crashed into.
- **A planned move loses nothing**: unmount, `drain`, then hand the disk over.
- **A process crash on a surviving host loses nothing.** The image and the intent map are on local
  disk; the restarted worker re-uploads every region still marked dirty, then re-attaches the device.

Switching policies:

| Change | Effect |
|---|---|
| `local` → `remote` | The next flush waits for everything sealed so far, including the backlog, to be committed. Later flushes behave as `remote`. |
| `remote` → `local` | Takes effect immediately. |

`max_lag` can be changed at runtime (`ctl lag <id> <MiB>`). The upload lag (`queued_bytes`) is
reported by `ctl stats`; a platform should alert when it stays above `max_lag`.

### 3.3 Write path and the log

| I/O | Effect |
|---|---|
| **READ** | io_uring read from the local image. |
| **WRITE** | Make the region's intent bit durable if this is the first write into a clean region since its last clear. Then an io_uring write to the image, then a copy into the open segment. |
| **FLUSH** | io_uring `fdatasync` of the image, seal the open segment, then wait according to the policy. |
| **FUA write** | A write followed by a flush. |
| **DISCARD / WRITE_ZEROES** | Punch a hole in the image and record a discard entry in the open segment. |

Upload pipeline:

- A process-wide pool of upload workers (default 16) serves all disks.
- Each disk has at most `WINDOW` (8) segments in flight beyond its committed prefix.
- Out-of-order completions are committed only once their predecessors are.
- Segments are compressed with zstd level 1, unless a 256 KiB sample shows the data does not
  compress, in which case they are stored raw.

Object layout:

```
<prefix>/epoch/<e:010>          empty; created with If-None-Match: * by the writer that owns epoch e
<prefix>/seg/<e:010>/<s:012>    segment s of epoch e, or the 4-byte tombstone "TOMB"
```

Segment format (inside the object, after a one-byte codec: 0 = raw, 1 = zstd):

```
"BGS1" | epoch u64 | seq u64 | n u32 | n × (kind u8, offset u64, len u32) | payload of the data entries
kind 0 = data, 1 = discard
```

Replay rule:

- Within an epoch, only the contiguous prefix `0, 1, 2, …` is valid. Replay stops at the first gap
  or the first tombstone, whichever comes first.
- Epochs are replayed in ascending order.
- A segment whose header disagrees with its key is rejected.

### 3.4 Fencing

Opening a disk:

1. List `<prefix>/epoch/` and take the highest epoch `L`.
2. Create `<prefix>/epoch/<L+1>` with `If-None-Match: *`. If it already exists, go back to step 1.
3. List `<prefix>/seg/<L>/` and write a tombstone, create-only, into every free sequence number
   from the first gap up to `max + FENCE_SPAN`, where `FENCE_SPAN = 2 × WINDOW`.
   - The old writer has at most `WINDOW` uploads in flight beyond its committed prefix, so all of
     them collide.
   - If a tombstone PUT finds its key already taken, the old writer just landed that segment. That
     is still safe: replay stops at the first tombstone either way.
4. Replay the segments that are needed (§3.5), then serve the disk under epoch `L+1`.

When an upload of the old writer finds its key taken (HTTP 412 or 409), the old writer marks the
disk fenced. All further I/O on it fails with `EIO`, and it logs `FENCED`.

- Under the `local` policy, flushes the old writer acknowledged before discovering the fence are
  lost. That is the meaning of `local`, which is why a platform must fence the old host (power off,
  STONITH) before handing a disk over.
- Object stores without `If-Match` support can still run this protocol: Aliyun OSS, for example,
  offers only `x-oss-forbid-overwrite`, and the protocol needs only create-only writes. This has not
  been tested against OSS.

### 3.5 Open and restore

Opening a disk on a host:

- **Fresh host** (no local image): replay every epoch below the new one, fetching segments in
  parallel batches of 16 and applying them in order.
- **Host that wrote the disk before**: replay only epochs newer than its own. Its own segments are
  already in its image.
- **Divergence**: if the host has un-uploaded writes (intent bits set) and another host has written
  since, opening fails with a "diverged" error and nothing is overwritten.

Then, if intent bits are set (an earlier crash), the server re-uploads those regions before the
device is attached:

- only data extents are read (`SEEK_DATA` / `SEEK_HOLE`); holes become discards;
- everything goes into **one segment**, committed atomically.
  - If the re-upload were split, a host lost partway through would leave a log prefix that mixes
    regions at their crash-time state with regions at an older state, which is not a state the disk
    was ever in. The torture test exercises this.
  - The segment's size is the dirty data, which write-intent tracking keeps close to the upload lag
    at the time of the crash.

### 3.6 Write-intent regions

- A bit per 4 MiB region, stored in `<dir>/intent`.
- A bit is set durably before the first write into a clean region.
- A bit is cleared when the last segment that wrote the region is committed and no write into it is
  in flight. Clearing is persisted lazily, at most once a second or when the queue drains; a stale
  set bit only costs an extra re-upload.
- The dirty set after a crash therefore tracks the un-uploaded tail, not everything written since
  the disk was last idle.

### 3.7 Failure semantics

| Failure | What the consumer sees | Handling |
|---|---|---|
| Worker process crashes | I/O stalls (ublk user recovery), no errors | The supervisor restarts the worker at once; the worker re-uploads the dirty regions, then re-attaches |
| No worker comes back within `--orphan-timeout` (default 30 s) | I/O fails with `EIO`; the filesystem shuts down | The supervisor stops the devices, deletes them asynchronously (the kernel finishes once the filesystem is unmounted), and exits with status 1 |
| The whole service is killed (supervisor and worker) | I/O stalls until the service is started again | A new `serve` re-attaches every device that is waiting for a server |
| `SIGTERM` | nothing; the devices stay recoverable | The worker drains every disk's upload queue (bounded at 60 s per disk), then exits |
| Object store unavailable | `local`: none until the lag exceeds `max_lag`, then writers wait. `remote`: `fsync` blocks | Uploads retry with backoff up to 5 s; errors are logged |
| Fenced by a newer writer | `EIO` on every I/O | `FENCED` log line; the disk must be given up |
| Local image or intent file lost | disk unusable on this host | Restore on any host from the log; under `local`, the un-uploaded tail is lost |
| Host lost | — | Another host opens the disk; under `local`, up to `max_lag` of tail is lost |

## 4. Process model and resources

- **Processes**: `blockgw serve` is a supervisor with no threads of its own. It runs
  `blockgw worker CONFIG` and polls the state of the configured ublk devices every 100 ms.
  - A starting worker attaches each device according to its kernel state: QUIESCED or FAIL_IO →
    recover; DEAD → delete and add; missing → add; LIVE → refuse, because another server owns it.
  - `--no-supervisor` runs the worker directly, for an external service manager.
- **Threads**: one ublk queue thread per disk queue, plus one dispatcher thread per disk; one
  control-socket thread and 16 upload workers per process.
- **Memory**: 15.5 MB for 16 idle disks.
  - Under load it is dominated by I/O buffers (queue depth 32 × 256 KiB per queue), segment buffers
    (up to 16 MiB open plus the queued bytes) and allocator caches.
  - Run with `MALLOC_ARENA_MAX=2`; without it, glibc kept about 550 MB of per-thread arenas after a
    5.5 GB write burst.
- **Object-store requests**: one PUT per sealed segment and nothing when idle. Opening a disk does
  `LIST`s plus one `GET` per replayed segment.
  - Under `remote`, every flush that has new data seals a segment, so the PUT count follows the
    fsync count. A fsync-heavy workload under `remote` pays one request per fsync.

## 5. Small and large files

- **Small files**: the filesystem on top packs them into blocks in the kernel, and blockgw packs
  blocks into segments of up to 16 MiB. Request count follows bytes and flushes, not file count.
  Hot and cold reads come from the local image.
- **Large files**: sequential writes fill segments quickly and are uploaded with up to 8 PUTs in
  flight per disk. Incompressible data skips compression.
  - Sustained write throughput is bounded by upload bandwidth once the lag limit is reached: about
    200 MB/s from a 3-vCPU host to S3.
  - Raising `max_lag` absorbs larger bursts, at the price of a larger loss window under `local`.

## 6. Not implemented yet

1. Lazy cold reads: today the full image lives on the host and a restore downloads every segment.
2. Extent-map checkpoints. Restore time grows with segment count.
3. Garbage collection and repacking: dead segments are never deleted.
4. Encryption at rest.
5. Standby replication of unflushed writes.

## 7. Alternatives considered

| Alternative | Why it is not the main path |
|---|---|
| dm-cache or dm-writecache in front of an object-store-backed device | Write-back order is chosen by the kernel's policy, unrelated to filesystem flushes, so the object store would not hold a crash-consistent state. |
| dm-thin snapshots plus `thin_delta` uploads | Consistent only at freeze-snapshot points, so it stalls the workload and offers no per-flush durability. |
| dm-clone for lazy hydration | Not built in Debian kernels. |
| DRBD to a peer | Mature synchronous replication, but it needs a peer and a DRBD resource per disk. Reasonable as another implementation of the `remote` policy for a few critical disks. |
| VDO | Deduplicates and compresses locally; it does not reduce object-store cost. |
