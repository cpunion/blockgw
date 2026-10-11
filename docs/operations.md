# Operations

## Running as a service

[examples/blockgw.service](../examples/blockgw.service) is a systemd unit:

- **Before starting** it loads `ublk_drv` and validates the config (`blockgw check-config`).
- **Supervision**: `blockgw serve` restarts a crashed worker within about 200 ms. If no worker can
  serve the disks within `--orphan-timeout`, it stops them, so I/O fails with EIO instead of hanging,
  and exits 1. systemd then restarts the service.
- **Stopping**: `SIGTERM` drains every disk's upload queue, then exits. The devices stay attached and
  are re-attached by the next start, so an upgrade is: install the new binary, then
  `systemctl restart blockgw`. Consumers see a stall, not an error.
- **Moving a disk** to another host: unmount, `blockgw ctl SOCK drain <id>`, stop the service, and
  start a server with the same `prefix` on the new host.
- **Removing a disk**: unmount, then `blockgw del <id>`, and remove it from the config.

## Control socket

`blockgw ctl <control_socket> <command>`:

| Command | Effect |
|---|---|
| `stats` / `stats json` | per-disk counters, as text or JSON |
| `metrics` | the same Prometheus text the HTTP endpoint serves |
| `policy <id> local\|remote` | switch the durability policy at runtime |
| `lag <id> <MiB>` | change the upload lag limit for the `local` policy |
| `drain <id>` | seal the open segment, wait until everything is committed, then write a checkpoint |
| `gc <id>` | drain, checkpoint, then run one garbage-collection round now |
| `attach <disk JSON>` | open a disk at runtime (needs `devices_dir`); replies once `/dev/ublkb<id>` is ready |
| `rekey <id>` | after a key rotation, rewrite everything still sealed with an old key; reports when none is left |
| `detach <id> [purge]` | stop serving a disk attached with `attach`, after its writes are in the store; `purge` also removes its local data |

## Attaching and detaching disks at runtime

Set `"devices_dir": "/var/lib/blockgw/disks"` in the config. Disks can then come and go without
restarting the service; the static `devices` list may stay empty.

```sh
blockgw ctl /run/blockgw.sock attach '{"id":12,"size_gb":20,"dir":"/var/lib/blockgw/img/12","prefix":"envs/12","policy":"local"}'
# ... mount /dev/ublkb12, use it, unmount ...
blockgw ctl /run/blockgw.sock detach 12          # keep the local image: a later attach is instant
blockgw ctl /run/blockgw.sock detach 12 purge    # park: the disk lives only in the object store
```

- `attach` takes the same JSON as an entry of `devices`. It writes `<devices_dir>/<id>.json`
  first, so a worker restart re-attaches the disk. Ids, prefixes and local dirs must differ from
  every attached disk.
- A disk whose local image is gone (purged, or a new host) is restored lazily: the device is
  served at once and blocks are fetched as they are read.
- `detach` refuses while the device is mounted or held open, in any mount namespace (it checks
  with an exclusive open). It then waits for the upload backlog (up to 60 s; if that fails the
  disk stays attached), stops the device, uploads anything that slipped in, writes a checkpoint,
  deletes the device and removes the disk file.
- Disks from the config file's `devices` cannot be detached at runtime; edit the config instead.
- The supervisor re-reads `devices_dir`; only devices that exist count toward the orphan timeout,
  so a disk being attached does not.

## Metrics

Set `"metrics_addr": "127.0.0.1:9187"` in the config to serve Prometheus metrics at `GET /metrics`.
Per-disk series carry a `disk` label.

| Metric | Type | Meaning |
|---|---|---|
| `blockgw_info{version}` | gauge | build version |
| `blockgw_process_resident_memory_bytes` | gauge | worker RSS |
| `blockgw_epoch` | gauge | epoch the server writes the disk under |
| `blockgw_policy_remote` | gauge | 1 = `remote`, 0 = `local` |
| `blockgw_fenced` | gauge | 1 once a newer writer took the disk over |
| `blockgw_committed_segments` | gauge | segments committed in this epoch |
| `blockgw_upload_lag_bytes` | gauge | sealed bytes not yet committed: the loss window under `local` |
| `blockgw_open_segment_bytes` | gauge | bytes written since the last seal |
| `blockgw_max_lag_bytes` | gauge | configured lag limit |
| `blockgw_puts_total`, `blockgw_put_bytes_total`, `blockgw_raw_bytes_total` | counter | uploads; object bytes after compression; segment bytes before it |
| `blockgw_upload_errors_total` | counter | failed upload attempts (they are retried) |
| `blockgw_upload_seconds_total` | counter | time in successful uploads |
| `blockgw_flushes_total` | counter | flushes handled |
| `blockgw_flush_wait_seconds_total` | counter | time flushes waited for uploads (`remote`, or lag backpressure) |
| `blockgw_intent_syncs_total` | counter | write-intent bitmap fdatasyncs |
| `blockgw_lag_waits_total` | counter | times writers or flushes waited for the lag to shrink |
| `blockgw_gc_deleted_segments_total`, `blockgw_gc_deleted_bytes_total` | counter | segment objects, and their bytes, deleted by GC |
| `blockgw_gc_repacked_bytes_total` | counter | live bytes rewritten out of sparse segments |
| `blockgw_absent_blocks` | gauge | 4 KiB blocks not fetched yet after a lazy restore (0 = fully hydrated) |
| `blockgw_fetched_bytes_total` | counter | bytes fetched on demand or by background hydration |
| `blockgw_fetch_errors_total` | counter | failed fetch attempts (a read fails with EIO after retries) |

Suggested alerts:

| Condition | Meaning |
|---|---|
| `blockgw_fenced == 1` | another host owns the disk; this server must give it up |
| `blockgw_upload_lag_bytes > blockgw_max_lag_bytes` for more than a few minutes | uploads are not keeping up, and the loss window is growing |
| `rate(blockgw_upload_errors_total[5m]) > 0` for a while | the object store is unreachable or rejecting writes |
| `rate(blockgw_fetch_errors_total[5m]) > 0` | lazy reads are failing: the store is unreachable, or objects are missing |
| `rate(blockgw_flush_wait_seconds_total[5m]) / rate(blockgw_flushes_total[5m])` rising | fsync latency is rising: `remote` policy, or backpressure |
| supervisor log lines `worker exited` or `orphan timeout` | the worker is crashing |

## Per-disk settings

| Key | Default | Meaning |
|---|---|---|
| `policy` | `local` | durability policy at start (switchable at runtime) |
| `max_lag_mb` | 16 | upload lag allowed under `local` before writers wait |
| `seal_interval_ms` | 1000 | seal the open segment once its oldest write is this old; under `local` flushes do not seal, so this sets the PUT rate and adds to the loss window (0 = seal at every flush) |
| `checkpoint_every` | 64 | committed segments between checkpoints (also every 60 s if anything committed) |
| `checkpoint_mb` | 64 | also checkpoint after this much data written since the last checkpoint; GC keeps up to two checkpoint intervals of segments, so this bounds the store at about live data + 2 × `checkpoint_mb` plus what repack has not caught up with |
| `gc` | `true` | run garbage collection after each checkpoint |
| `repack_ratio` | 0.5 | repack segments whose live fraction is below this |
| `repack_budget_mb` | 64 | live data rewritten per GC round |
| `lazy` | `true` | on restore, serve the device at once and fetch blocks on first read |
| `hydrate_mbps` | 64 | background hydration rate after a lazy restore (0 = on demand only) |
| `key_file` | none | encrypt every object of the disk with the 32-byte key in this file |
| `old_key_files` | `[]` | earlier keys, used only to read objects sealed before a rotation |
| `depth`, `queues` | 32, 1 | ublk queue depth and number of queues |

## Encryption keys

```sh
install -m 600 /dev/null /etc/blockgw/keys/d0.key
head -c 32 /dev/urandom > /etc/blockgw/keys/d0.key   # or 64 hex digits
```

- Set `"key_file": "/etc/blockgw/keys/d0.key"` on the disk **before its first start**. An existing
  disk's key cannot be added, changed or removed: the server refuses to open a disk whose objects
  do not match its key setting.
- The file must not be readable by group or others; `check-config` and the server refuse it
  otherwise.
- Keep a copy of the key outside the host (a secrets manager). Without it the disk cannot be
  restored anywhere.
- One key may be shared by several disks.
- **Rotating a key**: give the disk the new key as `key_file` and the old one in
  `old_key_files`, re-attach it (or restart), run `blockgw ctl SOCK rekey <id>` and wait for
  "no segment or checkpoint needs an old key any more"; then remove `old_key_files`. Until then,
  keep the old key: objects sealed with it are still needed.

## Logs

Logs go to stderr as `2026-10-11T13:00:00.123Z INFO disk 3: ...`. Set the level with
`BLOCKGW_LOG=error|warn|info|debug` (default `info`).

| Level | Lines |
|---|---|
| `info` | disk open, recovery, ready, drain |
| `warn` | upload retries, supervisor restarts |
| `error` | `FENCED`, failed disk threads |
