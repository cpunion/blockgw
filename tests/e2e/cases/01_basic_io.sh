# Small files + a large file survive unmount/remount; segments get committed; nothing is fenced.
cfg=$(write_config a 1 4)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
gen_tree "$W/m/tree" 3000 1
rand_file "$W/big.src" $((64 << 20)); cp "$W/big.src" "$W/m/big.bin"
sync -f "$W/m"
h=$(tree_hash "$W/m/tree")
umount "$W/m"; remount "$id" "$W/m"
assert_eq "$(tree_hash "$W/m/tree")" "$h" "tree after remount"
assert_eq "$(file_hash "$W/m/big.bin")" "$(file_hash "$W/big.src")" "big file after remount"
drain "$cfg" "$id"
assert_gt "$(stat "$cfg" "$id" committed_seq)" 0 "segments committed"
assert_eq "$(stat "$cfg" "$id" fenced)" false
assert_eq "$(stat "$cfg" "$id" queued_bytes)" 0 "queue drained"
