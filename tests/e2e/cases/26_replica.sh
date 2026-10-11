# Standby replication: under `local` with a replica, fsync returns once the standby has the data.
# Uploads are held back, the primary host is "lost" (processes killed, local image deleted), and a
# new host opening the disk takes the acknowledged writes over from the standby's journal. With the
# standby down, fsync falls back to waiting for the object store.
port=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1])")
start_replica() { setsid "$BLOCKGW" replica --listen 127.0.0.1:$port --dir "$W/journal" >> "$W/replica.log" 2>&1 < /dev/null & echo $! > "$W/replica.pid"; echo $! >> "$W/pids"; sleep 0.3; }
start_replica
head -c 32 /dev/urandom > "$W/k"; chmod 600 "$W/k"
export BLOCKGW_FAILPOINTS_FILE=$W/failpoints
echo "upload=error" > "$BLOCKGW_FAILPOINTS_FILE"   # nothing reaches the object store
cfg=$(REPLICA=127.0.0.1:$port KEY_FILE=$W/k write_config a 1 2)
start_server "$cfg"; id=$(cfg_ids "$cfg")
for _ in $(seq 1 100); do grep -q "replicating to" "${cfg%.json}.log" && break; sleep 0.05; done
t0=$(now_ms)
dd if=/dev/urandom of="$W/payload" bs=1M count=8 status=none
dd if="$W/payload" of="$(blkdev $id)" bs=1M seek=64 oflag=direct conv=fsync status=none || fail "fsync failed with the standby up"
log "metric: 8 MiB written and fsynced via the standby in $(( $(now_ms) - t0 )) ms (uploads blocked)"
hp=$(file_hash "$W/payload")
assert_eq "$(stat "$cfg" "$id" committed_seq)" 0 "nothing committed to the object store"
kill9_all "$cfg"; rm -rf "$W/a"   # the primary host is gone
: > "$BLOCKGW_FAILPOINTS_FILE"
r=$(REPLICA=127.0.0.1:$port KEY_FILE=$W/k DISK_KEYS="a-d0" write_config r 1 2)
start_server "$r"; rid=$(cfg_ids "$r")
grep -q "segments from the replica's journal" "${r%.json}.log" || fail "the new host did not take over the journal"
log "note: $(grep -o "uploaded [0-9]* segments from the replica's journal" "${r%.json}.log" | head -1)"
assert_eq "$(dd if="$(blkdev $rid)" bs=1M skip=64 count=8 iflag=direct status=none | sha256sum | cut -c1-16)" "$hp" "acknowledged writes survive the loss of the primary host"
# the standby goes away: fsync still succeeds, by waiting for the object store
kill -9 "$(cat "$W/replica.pid")"
dd if=/dev/urandom of="$(blkdev $rid)" bs=1M count=2 seek=128 oflag=direct conv=fsync status=none || fail "fsync failed with the standby down"
grep -q "replica 127.0.0.1:$port unavailable" "${r%.json}.log" || fail "degraded mode not logged"
