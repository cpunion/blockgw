# Bounded local cache (cache_mb): once everything written is committed, cold regions are dropped
# from the local image and fetched again when read; data stays correct across a worker crash and a
# restart, and the image stays near the budget.
cached() { ctl "$1" stats json | python3 -c "import json,sys; d=json.load(sys.stdin)['disks'][0]; print(d['$2'])"; }
cfg=$(CACHE_MB=48 write_config a 1 2)
start_server "$cfg"; id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
rand_file "$W/m/big.bin" $((160 << 20)); gen_tree "$W/m/tree" 600 25
hb=$(file_hash "$W/m/big.bin"); ht=$(tree_hash "$W/m/tree")
sync -f "$W/m"
for _ in $(seq 1 150); do [ "$(cached "$cfg" cached_bytes)" -le $((48 << 20)) ] && break; sleep 0.2; done
c=$(cached "$cfg" cached_bytes); e=$(cached "$cfg" evicted_bytes)
log "metric: local image $((c >> 20)) MiB after writing 160+ MiB with cache_mb=48; evicted $((e >> 20)) MiB"
assert_lt "$c" $(((48 << 20) + 1)) "local image within the budget"
assert_gt "$e" $((100 << 20)) "evicted bytes"
echo 3 > /proc/sys/vm/drop_caches
assert_eq "$(file_hash "$W/m/big.bin")" "$hb" "big file read back after eviction"
assert_eq "$(tree_hash "$W/m/tree")" "$ht" "tree read back after eviction"
# read across a worker crash
echo 3 > /proc/sys/vm/drop_caches
( file_hash "$W/m/big.bin" > "$W/h1" ) & rp=$!
sleep 0.3; kill9_worker "$cfg"
wait $rp
assert_eq "$(cat "$W/h1")" "$hb" "big file read across a worker crash"
umount_ok "$W/m"
# a full restart keeps the bitmap: evicted blocks are fetched, local ones are not
kill9_all "$cfg"; start_server "$cfg"
remount "$id" "$W/m"
assert_eq "$(file_hash "$W/m/big.bin")" "$hb" "big file after a restart"
umount_ok "$W/m"
