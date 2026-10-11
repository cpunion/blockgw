# kill -9 while a small-file workload runs through the page cache; after recovery the workload
# completes and the result is identical to a clean run.
cfg=$(write_config a 1 4)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
gen_tree "$W/src" 4000 4
h=$(tree_hash "$W/src")
( cp -a "$W/src" "$W/m/copy"; sync -f "$W/m"; echo $? > "$W/cp.rc" ) &
cp_pid=$!
sleep 0.7
kill9_server "$cfg"; start_server "$cfg" --recover
wait $cp_pid
assert_eq "$(cat "$W/cp.rc")" 0 "copy + sync exit status"
assert_eq "$(tree_hash "$W/m/copy")" "$h" "tree after crash"
umount "$W/m"; drain "$cfg" "$id"
DISK_KEYS="a-d0" r=$(write_config r 1 4)
start_server "$r"
assert_eq "$(file_hash "$(image r 0)")" "$(file_hash "$(image a 0)")" "object store converged"
