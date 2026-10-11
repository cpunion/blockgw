# SIGTERM to the service drains every disk's upload queue before exiting, so the object store holds
# everything the disk acknowledged (verified by a restore).
export BLOCKGW_FAILPOINTS="upload=sleep:300"   # slow uploads so a backlog exists at SIGTERM
cfg=$(write_config a 1 4)
start_server "$cfg"
id=$(cfg_ids "$cfg")
ctl "$cfg" lag "$id" 1024 >/dev/null
mkfs_mount "$id" "$W/m"
rand_file "$W/m/f.bin" $((48 << 20))
umount_ok "$W/m"                               # local policy: flushes return before the uploads finish
q=$(stat "$cfg" "$id" queued_bytes); log "metric: queued at SIGTERM: $q bytes"
sup=$(cat "${cfg%.json}.pid")
kill -TERM "$sup"
for _ in $(seq 1 600); do kill -0 "$sup" 2>/dev/null || break; sleep 0.1; done
kill -0 "$sup" 2>/dev/null && fail "service did not exit after SIGTERM"
grep -q "drained to seq" "${cfg%.json}.log" || fail "no drain on shutdown"
unset BLOCKGW_FAILPOINTS
DISK_KEYS="a-d0" r=$(write_config r 1 4)
start_server "$r"
assert_eq "$(file_hash "$(image r 0)")" "$(file_hash "$(image a 0)")" "object store holds everything after SIGTERM"
