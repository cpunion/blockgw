# Under `local`, a flush does not seal the open segment: a timer does, once its oldest write is
# seal_interval_ms old. Many small synchronous writes then cost a few PUTs instead of one each,
# and still reach the store within the interval without any drain. `remote` still seals (and
# waits) at every flush; seal_interval_ms=0 restores sealing at every flush under `local` too.
puts() { ctl "$1" stats json | python3 -c "import json,sys; print(json.load(sys.stdin)['disks'][0]['puts'])"; }
sync_writes() { dd if=/dev/urandom of="/dev/ublkb$1" bs=4096 count="$2" oflag=direct,dsync status=none; }   # every write is FUA
cfg=$(write_config a 1 2 local)
start_server "$cfg"; id=$(cfg_ids "$cfg")
p0=$(puts "$cfg"); sync_writes "$id" 300; sleep 1.6
p1=$(puts "$cfg")
log "metric: local, 1 s interval: 300 synchronous writes -> $((p1 - p0)) PUTs"
assert_lt $((p1 - p0)) 10 "PUTs for 300 synchronous writes under local"
h=$(dd if="/dev/ublkb$id" bs=4096 count=300 iflag=direct status=none | sha256sum | cut -c1-16)
kill9_all "$cfg"   # no drain: the timer must have sealed and uploaded everything
r=$(DISK_KEYS="a-d0" write_config r 1 2)
start_server "$r"; rid=$(cfg_ids "$r")
assert_eq "$(dd if="/dev/ublkb$rid" bs=4096 count=300 iflag=direct status=none | sha256sum | cut -c1-16)" "$h" "the writes reached the store within the interval"
z=$(SEAL_INTERVAL_MS=0 write_config z 1 2 local)
start_server "$z"; zid=$(cfg_ids "$z")
q0=$(puts "$z"); sync_writes "$zid" 300; sleep 0.5
log "metric: local, seal_interval_ms=0: 300 synchronous writes -> $(( $(puts "$z") - q0 )) PUTs"
assert_gt $(( $(puts "$z") - q0 )) 250 "PUTs with sealing at every flush"
m=$(write_config m 1 2 remote)
start_server "$m"; mid=$(cfg_ids "$m")
q0=$(puts "$m"); sync_writes "$mid" 100
log "metric: remote: 100 synchronous writes -> $(( $(puts "$m") - q0 )) PUTs"
assert_gt $(( $(puts "$m") - q0 )) 90 "remote seals at every flush"
