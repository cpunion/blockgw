# A fresh host restores a disk from the object store alone: byte-identical image, files intact.
cfg=$(write_config a 1 4)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
gen_tree "$W/m/tree" 2000 2
rand_file "$W/m/big.bin" $((48 << 20))
h=$(tree_hash "$W/m/tree"); hb=$(file_hash "$W/m/big.bin")
umount_ok "$W/m"                       # unmount writes too: drain after it
drain "$cfg" "$id"
a=$(file_hash "$(image a 0)")
r=$(DISK_KEYS="a-d0" write_config r 1 4)
t0=$(now_ms); start_server "$r"; log "metric: restore $(( $(now_ms) - t0 )) ms, $(last_start_log "$r" | grep -oE '(lazy restore|restored from the object store).*')"
assert_eq "$(dev_hash "$(cfg_ids "$r")")" "$a" "restored image"
rid=$(cfg_ids "$r")
remount "$rid" "$W/rm"
assert_eq "$(tree_hash "$W/rm/tree")" "$h" "restored tree"
assert_eq "$(file_hash "$W/rm/big.bin")" "$hb" "restored big file"
