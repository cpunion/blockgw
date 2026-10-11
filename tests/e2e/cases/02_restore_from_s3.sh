# A fresh host restores a disk from the object store alone: byte-identical image, files intact.
cfg=$(write_config a 1 4)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
gen_tree "$W/m/tree" 2000 2
rand_file "$W/m/big.bin" $((48 << 20))
h=$(tree_hash "$W/m/tree"); hb=$(file_hash "$W/m/big.bin")
umount "$W/m"                       # unmount writes too: drain after it
drain "$cfg" "$id"
a=$(file_hash "$(image a 0)")
DISK_KEYS="a-d0" r=$(write_config r 1 4)
t0=$(now_ms); start_server "$r"; log "metric: restore $(( $(now_ms) - t0 )) ms, $(last_start_log "$r" | grep -oE 'restored from the object store.*')"
assert_eq "$(file_hash "$(image r 0)")" "$a" "restored image"
rid=$(cfg_ids "$r")
remount "$rid" "$W/r"
assert_eq "$(tree_hash "$W/r/tree")" "$h" "restored tree"
assert_eq "$(file_hash "$W/r/big.bin")" "$hb" "restored big file"
