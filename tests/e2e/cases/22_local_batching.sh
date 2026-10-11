# Under `local`, a flush does not seal the open segment: a timer does, once its oldest write is
# seal_interval_ms old. Many small synchronous writes then cost a few PUTs instead of one each,
# and still reach the store within the interval without any drain. `remote` still seals (and
# waits) at every flush; seal_interval_ms=0 restores sealing at every flush under `local` too.
# segments: drain (seals what is open and waits for the uploads), then the committed count
segs() { drain "$1" "$2"; ctl "$1" stats json | python3 -c "import json,sys; print(json.load(sys.stdin)['disks'][0]['committed_seq'])"; }
sync_writes() { dd if=/dev/urandom of="$(blkdev $1)" bs=4096 count="$2" oflag=direct,dsync status=none; }   # every write is FUA
cfg=$(write_config a 1 2 local)
start_server "$cfg"; id=$(cfg_ids "$cfg")
p0=$(segs "$cfg" "$id"); sync_writes "$id" 300
sleep 1.6   # the timer seals; give the upload time to finish on a slow store
for _ in $(seq 1 100); do [ "$(stat "$cfg" "$id" queued_bytes)" = 0 ] && [ "$(stat "$cfg" "$id" open_bytes)" = 0 ] && break; sleep 0.2; done
h=$(dd if="$(blkdev $id)" bs=4096 count=300 iflag=direct status=none | sha256sum | cut -c1-16)
p1=$(ctl "$cfg" stats json | python3 -c "import json,sys; print(json.load(sys.stdin)['disks'][0]['committed_seq'])")
log "metric: local, 1 s interval: 300 synchronous writes -> $((p1 - p0)) segments"
assert_lt $((p1 - p0)) 10 "segments for 300 synchronous writes under local"
kill9_all "$cfg"   # no drain: the timer must have sealed and uploaded everything
r=$(DISK_KEYS="a-d0" write_config r 1 2)
start_server "$r"; rid=$(cfg_ids "$r")
assert_eq "$(dd if="$(blkdev $rid)" bs=4096 count=300 iflag=direct status=none | sha256sum | cut -c1-16)" "$h" "the writes reached the store within the interval"
z=$(SEAL_INTERVAL_MS=0 write_config z 1 2 local)
start_server "$z"; zid=$(cfg_ids "$z")
q0=$(segs "$z" "$zid"); sync_writes "$zid" 300; q1=$(segs "$z" "$zid")
log "metric: local, seal_interval_ms=0: 300 synchronous writes -> $((q1 - q0)) segments"
assert_gt $((q1 - q0)) 250 "segments with sealing at every flush"
m=$(write_config m 1 2 remote)
start_server "$m"; mid=$(cfg_ids "$m")
q0=$(segs "$m" "$mid"); sync_writes "$mid" 100; q1=$(segs "$m" "$mid")
log "metric: remote: 100 synchronous writes -> $((q1 - q0)) segments"
assert_gt $((q1 - q0)) 90 "remote seals at every flush"
