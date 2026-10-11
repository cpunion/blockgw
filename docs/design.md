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
| `local` | on the host's disk (image `fdatasync`) | 0.8–3 ms | the un-uploaded tail: up to `seal_interval_ms` of writes not yet sealed, plus up to `max_lag` sealed but not uploaded |
| `remote` | in the object store | about 59 ms (host about 9 ms from S3) | nothing acknowledged by `fsync` |

Under both policies:

- **The object store always holds a crash-consistent prefix.** A segment holds writes in arrival
  order and can be cut anywhere: a cut between two writes is a valid crash point, because a
  write that completed before another was issued always arrives first. Segments are committed in
  sequence order, so any committed prefix is a state the disk could have crashed into.
- **When segments are sealed**: under `remote`, at every flush (the flush then waits for the
  upload). Under both policies, when the open segment's oldest write is `seal_interval_ms` old
  (default 1000; one thread checks every disk every 100 ms) or the segment reaches 16 MiB. Under
  `local` a flush does not seal, so the writes of many flushes share one PUT: 300 synchronous
  4 KiB writes cost one PUT instead of 300 (e2e case `22_local_batching`). `seal_interval_ms: 0`
  restores sealing at every flush.
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
<prefix>/ckpt/<e:010>/<s:012>   checkpoint: the committed extent map after segment s of epoch e
```

Segment format v3. The layout lets a reader fetch only the frames it needs with ranged GETs:

```
header (48 B): "BGS3" | epoch u64 | seq u64 | committed u64 | n_entries u32 | n_frames u32 | frame_size u32 | meta_len u32 | crc32(meta) u32
meta:          n_entries × (kind u8, disk_off u64, len u32, data_off u64)          kind 0 = data, 1 = discard
               n_frames  × (obj_off u64, obj_len u32, raw_len u32, codec u8, crc32(raw) u32)
frames:        the data stream cut into 256 KiB frames, each raw or zstd, independently decodable
```

- Every entry is a multiple of the 4 KiB logical block size: the device advertises 4 KiB logical
  blocks, so all I/O is block aligned.
- `data_off` is the entry's position in the data stream; frame `i` covers bytes
  `[i × frame_size, i × frame_size + raw_len)` of it.
- Every frame carries a crc32 of its raw bytes, and the meta carries one too.
- `committed` attests how far the writer's log was committed when the segment was encoded: every
  seq of the epoch below it exists in the store. Sealing uses it to tell a lost segment from one
  that was still in flight (§3.4).
- v2 (`BGS2`, 40-byte header, no `committed`) is still read, as `committed = 0`.

### 3.3a Extent map and checkpoints

Each disk keeps a **committed extent map**:

- **Contents**: for every 4 KiB block, the segment and data block holding its committed contents,
  or nothing (a hole).
- **Consistency**: it is updated only when segments commit, in commit order, under the same lock
  that advances the committed position. A snapshot is therefore always consistent with a log
  position.
- **Storage**: a memory-mapped file of 8-byte entries; only touched pages are resident.

A **checkpoint** serializes the map: the segment table, plus runs of blocks that are consecutive in
the same segment, all zstd-compressed.

- **When**: written every 64 committed segments, or 60 s after the last one if anything committed,
  and after every `drain` and on SIGTERM.
- **Validity**: a checkpoint names a committed position. Tombstones only ever fill sequence numbers
  above the committed prefix, so no later fencing can invalidate a checkpoint.

Replay rule:

- Within an epoch, only the contiguous prefix `0, 1, 2, …` is valid. Replay stops at the first gap
  or the first tombstone, whichever comes first.
- Epochs are replayed in ascending order.
- A segment whose header disagrees with its key is rejected.

### 3.4 Fencing

Opening a disk:

1. List `<prefix>/epoch/` and take the highest epoch `L`.
2. Create `<prefix>/epoch/<L+1>` with `If-None-Match: *`. If it already exists, go back to step 1.
3. **Seal** every epoch up to `L` that is not sealed yet, oldest first. An epoch is sealed when its
   highest object is a tombstone. To seal epoch `E`, write a tombstone, create-only, into every
   free sequence number from the first gap up to `max + FENCE_SPAN`, where `FENCE_SPAN = 2 × WINDOW`.
   The first gap is searched from one past `E`'s newest checkpoint: holes below it were made by
   GC, not by uploads in flight, and need no tombstones.
   - The old writer has at most `WINDOW` uploads in flight beyond its committed prefix, so all of
     them collide.
   - If a tombstone PUT finds its key already taken, the old writer just landed that segment. That
     is still safe: replay stops at the first tombstone either way.
   - **Lost or in flight?** A gap is normally an upload that was in flight when the writer died, and
     gets a tombstone. But if any segment above the gap attests `committed` beyond it, the gap's
     segment had already been committed and was lost from the store: opening fails with "log
     incomplete" rather than sealing the hole and restoring a partial log. Segments above the first
     gap are at most the writer's upload window, so this costs a few ranged GETs, and none in the
     common case (no gap).
   - Sealing every unsealed epoch, not only `L`, matters when a claim crashed between writing its
     epoch marker and fencing: epoch `L` is then empty, and the writer of `L−1` would otherwise
     stay unfenced and could keep landing segments that a later restore replays. Oldest first, so
     a claim that crashes midway leaves a sealed prefix for the next one to extend.
4. Replay the segments that are needed (§3.5), then serve the disk under epoch `L+1`.

After step 3 every epoch below the new one is sealed, which lets restore detect missing objects
(§3.5).

When an upload of the old writer finds its key taken (HTTP 412 or 409), the old writer marks the
disk fenced. All further I/O on it fails with `EIO`, and it logs `FENCED`.

- Under the `local` policy, flushes the old writer acknowledged before discovering the fence are
  lost. That is the meaning of `local`, which is why a platform must fence the old host (power off,
  STONITH) before handing a disk over.
- Object stores without `If-Match` support can still run this protocol: Aliyun OSS, for example,
  offers only `x-oss-forbid-overwrite`, and the protocol needs only create-only writes. This has not
  been tested against OSS.

### 3.5 Open and restore

Opening a disk picks the newest checkpoint below the new epoch, plus the segments after it: the
rest of the checkpoint's epoch, then later epochs, each a contiguous prefix ending at the first gap
or tombstone. Then:

- **Fresh host, or another host has written since** (an epoch newer than this host's own holds
  data): the image is rebuilt.
  - Load the checkpoint into the map.
  - Fetch every live run from its segment with ranged GETs: 16 segments in parallel, with adjacent
    frames merged into one request.
  - Replay the later segments in full.

  After many overwrites, a restore downloads the live data, not the history: in the e2e suite,
  8 MiB fetched after 160 MiB were uploaded.
- **Same host, nobody else wrote**: the local image is authoritative. Only the map is rebuilt, from
  the checkpoint plus the metas of later segments (one small ranged GET each).
- **Divergence**: if the host has un-uploaded writes (intent bits set) and another host has written
  since, opening fails with a "diverged" error and nothing is overwritten.
- **Missing objects**: every epoch below the new one is sealed (§3.4), so each epoch's run must end
  at a tombstone. A run that breaks off at a gap means a segment is missing from the store (lost,
  deleted by hand, or a store copy taken carelessly). Opening fails with "log incomplete" instead
  of restoring a partial log; the same holds for a checkpoint whose epoch has no objects at all.
  In the newest epoch, sealing catches the loss through `committed` (§3.4). A loss of the very
  last segments of an epoch, with nothing written after them, cannot be told from uploads in
  flight; under `remote` such segments were never acknowledged unless they were committed, and a
  committed segment is followed by later ones only if the writer kept writing.

With `lazy` (the default), a rebuild does not download any data up front: see 3.5b.

The map is always rebuilt from the object store at open, never trusted from a local file that a
crash may have left half-written.

Then, if intent bits are set (an earlier crash), the server re-uploads those regions. The segment
is built before the device is attached, so no new write changes the image under it; its upload
then proceeds in the background. It is sealed first, so every later segment commits after it, and
the intent bits stay set until it commits. Waiting for the upload would hold the device for as long
as the dirty data takes to upload: against Cloudflare R2 over a home uplink, that exceeded the
30 s orphan timeout and failed I/O; now the disk is served again within 9 s there.

- only data extents are read (`SEEK_DATA` / `SEEK_HOLE`); holes become discards;
- everything goes into **one segment**, committed atomically.
  - If the re-upload were split, a host lost partway through would leave a log prefix that mixes
    regions at their crash-time state with regions at an older state, which is not a state the disk
    was ever in. The torture test exercises this.
  - The segment's size is the dirty data, which write-intent tracking keeps close to the upload lag
    at the time of the crash.

### 3.5a Garbage collection

After each checkpoint, a GC round runs against the last two checkpoints this server knows of. The
checkpoint the disk was opened from counts as the first.

Checkpoints are written every `checkpoint_every` segments (64), every `checkpoint_mb` of data
written by users (64 MiB), and every 60 s if anything was committed. GC keeps everything above the
older of the last two checkpoints, so the store holds up to two checkpoint intervals of segments
beyond the live data. With batched sealing (§3.2) segments are large, and a segment count alone let
that grow to 12–25× the live data in the soak test; the byte limit keeps it at 2–3×. Repack copies
do not count toward `checkpoint_mb`, or each GC round would trigger the next.

1. **Ownership**: if an epoch marker newer than the server's own exists, the server marks itself
   fenced and stops. Only the current owner deletes.
2. **Delete old checkpoints**: those older than the older of the two. This comes first: while a
   checkpoint exists, every segment it references exists. A reader that found a checkpoint, or a
   copy of the store that contains one, therefore never meets a hole in the log it needs (the
   torture tests copy live stores this way).
3. **Delete segments**: a data segment may be deleted when it is at or below the position of the
   *older* checkpoint and *neither* checkpoint references it.
   - Keeping two checkpoints' worth covers a reader that listed the older checkpoint just before
     the newer one was written.
   - **Tombstones (4-byte objects) are never deleted**: a fenced writer that is still running must
     keep colliding with them, and they mark sealed epochs (§3.4).
4. **Repack**: segments at or below the newer checkpoint whose live fraction is below
   `repack_ratio` (default 0.5) have their live blocks copied from the local image into the open
   segment, oldest first, within `repack_budget_mb` per round (default 64).
   - This runs under the open-segment lock, so no write can append between reading a block and
     appending its copy; a copy is never older than a write logged before it.
   - Once the copies commit, the old segment is no longer referenced and is deleted two checkpoints
     later.

**Replay after GC**: below a checkpoint, GC leaves gaps, so replay only requires contiguity *after*
the checkpoint position: `cs + 1, cs + 2, ...` in the checkpoint's epoch, and from 0 in later epochs.

The torture test runs with a checkpoint every 8 segments, so GC deletes and repacks throughout:
about 135 rounds per 60 s run. Every store copy still restores to an exact prefix of the writes.

### 3.5b Lazy restore

A lazy rebuild loads the checkpoint and the metas of later segments into the map, truncates the
image to an empty sparse file and serves the device at once: in the e2e suite, 145 ms for a disk
with 110 MiB of data. Each 4 KiB block is then *absent* (the map points at a segment) or *present*
(valid in the image; holes start present). A bitmap tracks this.

- **Reads** that touch absent blocks wait (on an eventfd, off the io_uring thread) while a pool of
  8 fetch workers pulls those blocks with ranged GETs of the frames that hold them, writes them
  into the image and marks them present.
- **Background hydration** walks the absent blocks in 1 MiB steps, rate-limited by `hydrate_mbps`
  (default 64; 0 = on demand only). When nothing is absent, the bitmap file is removed and the disk
  behaves exactly like a fully restored one.
- **Writes win over fetches.** A write marks its blocks *being written* before touching the image
  and present once the image write completes. A fetch fills only blocks that are absent and not
  being written, re-checked under the lock after the GET returns. So fetched (committed, older)
  data never overwrites a newer local write, even when the two race.
- **Crash safety of the bitmap.** It must never claim a block present whose data is not durable in
  the image. A flush takes a snapshot of the dirty bitmap pages *before* fdatasyncing the image,
  then writes and syncs the snapshot, and only then acknowledges the flush. A crash can therefore
  leave bits stale-absent, never stale-present: a block written (or fetched) since the last flush
  may be marked absent although its data is in the image.
- **Recovery** after a crash must not trust the bitmap for that reason. Treating a stale-absent
  written block as absent would drop that write while a later write to a present block is
  re-uploaded: a state the disk was never in. So, within dirty regions, recovery reads the image:
  - a data extent is never older than the block's committed version (it was fetched from it, or
    written since), so it is re-uploaded and marked present;
  - a hole that is present is uploaded as a discard;
  - a hole that is absent was never fetched, and its data is still in the store. This is
    unambiguous because a discard over absent blocks writes zeros to them instead of punching.
  The bitmap update is persisted after an image fdatasync, as at flush. e2e case
  `21_lazy_crash_order` and the takeover torture cover this; an earlier version that re-uploaded
  only present blocks failed both.
- **Repack** copies only runs whose blocks are all present (it reads them from the image).
- **Failures.** A fetch that fails is planned again from the current map (a local write may have
  superseded the block and GC deleted its old segment), up to 3 times; then the read fails with
  EIO and `blockgw_fetch_errors_total` counts it. Nothing is ever served from an unfetched block.

The torture test reads every restored copy through the device, so each crash-consistency check also
exercises on-demand fetching; e2e case `18_lazy_restore` covers reads across a worker crash and
hydration to completion.

### 3.5c Bounded local cache

With `cache_mb` set, a disk keeps at most that much of its data in the local image; the rest lives
only in the object store and is fetched when read (the lazy-restore path). Background hydration is
off for such a disk.

- **What may be dropped**: a 4 MiB region whose write-intent bit is clear and which has no write in
  flight, so every write into it is committed and the object store holds exactly what the image
  holds; and only blocks the extent map points at (unmapped blocks read as zeros anyway).
- **Order**: least recently read or written regions first, every second, until the image is at 90%
  of the budget (allocated blocks of the image file).
- **Races**, each closed by a lock the evictor holds while it drops a region:
  - the open-segment lock: GC repack and rekey copy blocks from the image under it, and re-check
    under it that the blocks are still present;
  - the regions lock: no write can start in the region;
  - the lazy lock: no fetch can fill a block, and no read can start. Reads register the region
    before checking presence and unregister after reading the image; the evictor skips regions with
    reads in flight, so a read that found its blocks present finds them still there.
- **Crash safety**: the image is fdatasync'd, the blocks are marked absent and the bitmap pages are
  written and synced, and only then is the hole punched. The bitmap may claim a block absent whose
  data is still there (a refetch), never present when it was punched.
- The presence file now always exists with every bit explicit. Before, an eager (`lazy: false`)
  restore removed it and the next open created an empty one, which reads as "everything absent":
  a restart then refetched the whole disk (correctly, but needlessly).

e2e case `25_cache_eviction` and CI torture jobs with 16 and 32 MiB budgets (working sets of
64 MiB and more) cover it.

### 3.6 Write-intent regions

- A bit per 4 MiB region, stored in `<dir>/intent`.
- A bit is set durably before the first write into a clean region.
- A bit is cleared when the last segment that wrote the region is committed and no write into it is
  in flight. Clearing is persisted lazily, at most once a second or when the queue drains; a stale
  set bit only costs an extra re-upload.
- The dirty set after a crash therefore tracks the un-uploaded tail, not everything written since
  the disk was last idle.

### 3.6a Encryption at rest

With `key_file` set, every object of the disk (segments, checkpoints, epoch markers, tombstones) is
sealed by a store wrapper before it leaves the host, so the log format above is unchanged and
compression still runs before encryption.

```text
object: "BGE1" | nonce prefix (16 random bytes) | plaintext length u64 | chunk 0 | chunk 1 | ...
chunk i: XChaCha20-Poly1305 of plaintext bytes [i * 64 KiB, (i + 1) * 64 KiB), plus a 16-byte tag
  nonce = nonce prefix | i;  associated data = object key | the 28-byte header
```

- **Ranged reads still work.** A read fetches whole 64 KiB chunks around the requested range.
  Each object's header is cached after its first read, and segment reads start with the meta at
  offset 0, so encryption adds no request. Overhead: 28 bytes per object plus 16 bytes per 64 KiB.
- **Nonces never repeat**: the 128-bit random prefix is drawn per object, and the chunk index makes
  it unique within it. Nonces do not depend on the key name, so an upload retried with different
  content (after a crash) cannot reuse a nonce.
- **Integrity and binding.** The object key and the header are authenticated. An object copied
  to another key (another log position, or another disk), chunks reordered, a truncated object or
  one sealed under another key all fail to decrypt.
- **Fail closed.** A disk with a key refuses objects that are not encrypted: an attacker who can
  write to the bucket cannot inject plaintext segments. A disk without a key reports "the object is
  encrypted, but this disk has no key_file" instead of misreading it. Either way, opening fails
  before the device is attached.
- **Key handling**: 32 bytes per disk, read from a file (64 hex digits or raw) that must not be
  accessible by group or others. One key may serve several disks; the key name in the associated
  data keeps their objects apart. Losing the key loses the disk.
- **Rotation**: open the disk with the new `key_file` and the old one in `old_key_files`
  (decrypt only). New objects use the new key; reads try the keys in turn and remember per object
  which one worked. `ctl rekey <id>` then fetches any absent blocks, rewrites every live block still
  held by a segment of an earlier epoch, checkpoints twice so GC deletes those segments and the old
  checkpoints, and checks that every remaining segment and checkpoint opens with the new key. After
  that the old key can be dropped. Tombstones and epoch markers keep the old key; they are never
  read.
- **A server that cannot read the log changes nothing.** Before claiming an epoch (which fences
  whoever serves the disk now), opening reads the newest checkpoint, or the newest segment. A wrong
  or missing key fails there; it used to fail only after the claim, so a misconfigured host could
  fence out a healthy writer.
- Listings report plaintext sizes, so size-based checks (tombstone detection) are unchanged.
- **Not hidden**: object names, sizes and timing (how much and when a disk writes) remain visible
  to the store. The local image on the host is not encrypted by blockgw (use dm-crypt or an
  encrypted host disk).

The torture test runs in CI with `--encrypt` as well (remote policy), and e2e case
`19_encryption` checks that no plaintext reaches the store and that wrong or missing keys fail.

### 3.7 Failure semantics

| Failure | What the consumer sees | Handling |
|---|---|---|
| Worker process crashes | I/O stalls (ublk user recovery), no errors | The supervisor restarts the worker at once; the worker queues the dirty regions for re-upload as one segment, then re-attaches |
| No worker comes back within `--orphan-timeout` (default 30 s) | I/O fails with `EIO`; the filesystem shuts down | The supervisor stops the devices, deletes them asynchronously (the kernel finishes once the filesystem is unmounted), and exits with status 1 |
| The whole service is killed (supervisor and worker) | I/O stalls until the service is started again | A new `serve` re-attaches every device that is waiting for a server |
| `SIGTERM` | nothing; the devices stay recoverable | The worker drains every disk's upload queue (bounded at 60 s per disk), then exits |
| Object store unavailable | `local`: none until the lag exceeds `max_lag`, then writers wait. `remote`: `fsync` blocks | Uploads retry with backoff up to 5 s; errors are logged |
| Fenced by a newer writer | `EIO` on every I/O | `FENCED` log line; the disk must be given up |
| Wrong, missing or unexpected key | the disk is not attached | opening fails with a decryption or "no key_file" error; nothing is served |
| A segment or checkpoint missing from the store | the disk is not attached | opening fails with "log incomplete"; a partial log is never restored |
| Local image or intent file lost | disk unusable on this host | Restore on any host from the log; under `local`, the un-uploaded tail is lost |
| Host lost | — | Another host opens the disk; under `local`, up to `max_lag` of tail is lost |

## 4. Process model and resources

- **Processes**: `blockgw serve` is a supervisor with no threads of its own. It runs
  `blockgw worker CONFIG` and polls the state of the configured ublk devices every 100 ms.
  - A starting worker attaches each device according to its kernel state: QUIESCED or FAIL_IO →
    recover; DEAD → delete and add; missing → add; LIVE → refuse, because another server owns it.
  - `--no-supervisor` runs the worker directly, for an external service manager.
  - With `devices_dir`, disks are attached and detached at runtime (`ctl attach` / `detach`); each
    is a JSON file there, so a restarted worker re-attaches the same set. The supervisor re-reads
    the set and counts only devices that exist toward the orphan timeout.
- **Threads**: one ublk queue thread per disk queue, plus one dispatcher thread per disk; one
  control-socket thread and 16 upload workers per process.
- **Memory**: 15.5 MB for 16 idle disks.
  - Under load it is dominated by I/O buffers (queue depth 32 × 256 KiB per queue), segment buffers
    (up to 16 MiB open plus the queued bytes) and allocator caches.
  - blockgw limits glibc to two malloc arenas at startup (`mallopt(M_ARENA_MAX, 2)`). Without
    that, glibc kept about 550 MB of per-thread arenas after a 5.5 GB write burst, and the soak
    test settled at 110–140 MiB instead of 50–56 MiB for three busy disks.
- **Object-store requests**: one PUT per sealed segment and nothing when idle. Opening a disk does
  `LIST`s plus one `GET` per replayed segment.
  - Under `local`, a busy disk makes at most about one PUT per `seal_interval_ms` plus one per
    16 MiB written, whatever the fsync rate. At S3's $5 per million PUTs, a disk that is busy
    around the clock costs about $0.43 a day in requests with the default 1 s interval, against
    about $7.8 a day for the soak workload (18 fsyncs a second) when every flush sealed.
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

2. Standby replication of unflushed writes.

## 7. Alternatives considered

| Alternative | Why it is not the main path |
|---|---|
| dm-cache or dm-writecache in front of an object-store-backed device | Write-back order is chosen by the kernel's policy, unrelated to filesystem flushes, so the object store would not hold a crash-consistent state. |
| dm-thin snapshots plus `thin_delta` uploads | Consistent only at freeze-snapshot points, so it stalls the workload and offers no per-flush durability. |
| dm-clone for lazy hydration | Not built in Debian kernels. |
| DRBD to a peer | Mature synchronous replication, but it needs a peer and a DRBD resource per disk. Reasonable as another implementation of the `remote` policy for a few critical disks. |
| VDO | Deduplicates and compresses locally; it does not reduce object-store cost. |
