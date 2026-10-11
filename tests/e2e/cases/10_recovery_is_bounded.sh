# Regression: after mkfs (which discards the whole device) and a drained idle period, a crash must
# not mark the whole disk dirty. Re-upload work after kill -9 is bounded by the un-uploaded tail.
cfg=$(write_config a 1 8)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
rand_file "$W/m/f" $((32 << 20)); sync -f "$W/m"; drain "$cfg" "$id"; sleep 2
kill9_server "$cfg"; start_server "$cfg" --recover
n=$(last_start_log "$cfg" | grep -oE '[0-9]+ dirty regions' | grep -oE '^[0-9]+')
log "metric: dirty regions after an idle crash: $n (disk has $((8 << 30 >> 22)) regions)"
assert_lt "$n" 4 "dirty regions after an idle crash"
