# The supervisor restarts a crashed worker on its own; the new worker re-attaches the device
# (ublk user recovery) and re-uploads the tail. The writer sees no error.
cfg=$(write_config a 1 8)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
rand_file "$W/src.bin" $((192 << 20))
mark_dmesg
( dd if="$W/src.bin" of="$W/m/f.bin" bs=1M oflag=direct status=none; echo $? > "$W/dd.rc" ) &
dp=$!
sleep 1
t0=$(now_ms); kill9_worker "$cfg"
log "metric: supervisor restarted the worker and the disk was served again after $(( $(now_ms) - t0 )) ms"
wait $dp
assert_eq "$(cat "$W/dd.rc")" 0 "dd exit status"
assert_eq "$(file_hash "$W/m/f.bin")" "$(file_hash "$W/src.bin")" "file content"
assert_eq "$(kernel_errors)" 0 "kernel I/O errors"
grep -q "re-attaching the existing device" "${cfg%.json}.log" || fail "worker did not re-attach"
umount_ok "$W/m"; drain "$cfg" "$id"
DISK_KEYS="a-d0" r=$(write_config r 1 8)
start_server "$r"
assert_eq "$(file_hash "$(image r 0)")" "$(file_hash "$(image a 0)")" "object store converged"
