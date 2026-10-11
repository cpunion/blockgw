# kill -9 the server while an O_DIRECT writer has un-uploaded data: the writer sees no error, the
# file is intact, the tail is re-uploaded (the object store converges to the local image).
cfg=$(write_config a 1 8)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
rand_file "$W/src.bin" $((256 << 20))
mark_dmesg
( dd if="$W/src.bin" of="$W/m/f.bin" bs=1M oflag=direct status=none; echo $? > "$W/dd.rc" ) &
dp=$!
sleep 1
log "note: before kill: $(ctl "$cfg" stats | grep "^disk $id" | grep -oE 'committed_seq=[0-9]+ queued_bytes=[0-9]+ open_bytes=[0-9]+')"
t0=$(now_ms); kill9_all "$cfg"; start_server "$cfg"
log "metric: serving again after $(( $(now_ms) - t0 )) ms; $(last_start_log "$cfg" | grep -oE 'recovery re-uploaded.*|[0-9]+ dirty regions' | tr '\n' ' ')"
wait $dp
assert_eq "$(cat "$W/dd.rc")" 0 "dd exit status"
assert_eq "$(file_hash "$W/m/f.bin")" "$(file_hash "$W/src.bin")" "file content"
assert_eq "$(kernel_errors)" 0 "kernel I/O errors"
umount_ok "$W/m"; drain "$cfg" "$id"
DISK_KEYS="a-d0" r=$(write_config r 1 8)
start_server "$r"
assert_eq "$(dev_hash "$(cfg_ids "$r")")" "$(file_hash "$(image a 0)")" "object store converged to the local image"
