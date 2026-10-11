# Garbage collection: after many overwrites, GC rounds delete the dead segments and repack sparse
# ones; tombstones and the restorable state survive.
CKPT_EVERY=4 cfg=$(write_config a 1 2)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
count() { if [ "${E2E_STORE:-s3}" = dir ]; then find "$W/store" -path '*/seg/*' -type f ! -name '.tmp.*' | wc -l; else stat "$cfg" "$id" puts; fi; }
# GC deletes against the last two checkpoints, so it needs rounds with new commits in between
notes=""
for round in 1 2 3; do
  for i in $(seq 1 8); do
    head -c $((4 << 20)) /dev/urandom > "$W/m/f$((i % 3))"   # 3 files rewritten over and over
    sync -f "$W/m"
  done
  notes="$notes | $(ctl "$cfg" gc "$id")"
done
umount_ok "$W/m"
notes="$notes | $(ctl "$cfg" gc "$id")"
log "note:$notes"
deleted=$(stat "$cfg" "$id" gc_deleted_segments 2>/dev/null)
[ -z "$deleted" ] && deleted=$(ctl "$cfg" stats json | python3 -c "import json,sys; print(json.load(sys.stdin)['disks'][0]['gc_deleted_segments'])")
puts=$(stat "$cfg" "$id" puts)
log "metric: $puts segments uploaded, $deleted deleted by gc, $(count) left in the store"
assert_gt "$deleted" 5 "segments deleted by gc"
if [ "${E2E_STORE:-s3}" = dir ]; then
  assert_eq "$(count)" "$((puts - deleted))" "segment objects left = uploaded - deleted"
fi
DISK_KEYS="a-d0" r=$(write_config r 1 2)
start_server "$r"
assert_eq "$(file_hash "$(image r 0)")" "$(file_hash "$(image a 0)")" "restored image after gc"
