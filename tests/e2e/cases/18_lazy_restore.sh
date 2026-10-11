# Lazy restore: the device is served at once and blocks are fetched from the object store when
# first read; a crash mid-way keeps everything correct; background hydration then fetches the rest
# and nothing is left absent.
cfg=$(write_config a 1 2)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
rand_file "$W/m/big.bin" $((96 << 20)); gen_tree "$W/m/tree" 800 18
hb=$(file_hash "$W/m/big.bin"); ht=$(tree_hash "$W/m/tree")
umount_ok "$W/m"; drain "$cfg" "$id"
src=$(file_hash "$(image a 0)")
r=$(HYDRATE_MBPS=0 DISK_KEYS="a-d0" write_config r 1 2)   # on demand only
t0=$(now_ms); start_server "$r"; rid=$(cfg_ids "$r")
log "metric: lazy restore served the device after $(( $(now_ms) - t0 )) ms: $(last_start_log "$r" | grep -oE 'lazy restore.*')"
a0=$(ctl "$r" stats json | python3 -c "import json,sys;print(json.load(sys.stdin)['disks'][0]['absent_blocks'])")
assert_gt "$a0" 20000 "blocks absent right after the restore"
remount "$rid" "$W/rm" ro,norecovery   # read-only: the device must stay byte-identical to the source
assert_eq "$(tree_hash "$W/rm/tree")" "$ht" "small files read on demand"
a1=$(ctl "$r" stats json | python3 -c "import json,sys;print(json.load(sys.stdin)['disks'][0]['absent_blocks'])")
assert_lt "$a1" "$a0" "reading fetched blocks"
assert_gt "$a1" 10000 "only what was read is fetched (the big file is still absent)"
# crash the worker in the middle of reading the big file; the supervisor restarts it and the
# presence bitmap (persisted at flush) keeps it correct
( file_hash "$W/rm/big.bin" > "$W/h1" ) & rp=$!
sleep 0.3; kill9_worker "$r"
wait $rp
assert_eq "$(cat "$W/h1")" "$hb" "big file read across a worker crash"
umount_ok "$W/rm"
assert_eq "$(dev_hash "$rid")" "$src" "whole device after on-demand reads"
# background hydration: restart with hydration on and wait until nothing is absent
kill9_all "$r"
python3 -c "import json,sys; c=json.load(open(sys.argv[1])); c['devices'][0]['hydrate_mbps']=512; json.dump(c,open(sys.argv[1],'w'))" "$r"
start_server "$r"
for _ in $(seq 1 300); do
  a=$(ctl "$r" stats json | python3 -c "import json,sys;print(json.load(sys.stdin)['disks'][0]['absent_blocks'])")
  [ "$a" = 0 ] && break; sleep 0.1
done
assert_eq "$a" 0 "absent blocks after hydration"
drain "$r" "$rid"   # a flush persists the presence state
assert_eq "$(file_hash "$(image r 0)")" "$src" "hydrated image file"
