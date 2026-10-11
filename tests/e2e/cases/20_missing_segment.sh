# A segment missing from the store (lost, deleted by hand, a careless copy) must stop a restore with
# "log incomplete" instead of restoring a partial log; GC itself never leaves such a hole.
[ "${E2E_STORE:-s3}" = dir ] || skip "deletes an object directly: dir store only"
cfg=$(write_config a 1 2 remote)   # remote: every fsync'd write is committed when dd returns
start_server "$cfg"; id=$(cfg_ids "$cfg")
for i in 1 2 3 4 5 6; do
  dd if=/dev/urandom of="$(blkdev $id)" bs=1M count=4 seek=$((i * 8)) oflag=direct conv=fsync status=none
done
kill9_all "$cfg"
segs=$(find "$W/store" -path '*/seg/*' -type f -size +4c | sort)
n=$(echo "$segs" | wc -l)
assert_gt "$n" 3 "segments uploaded"
victim=$(echo "$segs" | sed -n "$((n / 2))p")
cp "$victim" "$W/victim"; rm "$victim"
r=$(DISK_KEYS="a-d0" write_config r 1 2)
out=$(timeout 60 "$BLOCKGW" serve "$r" --no-supervisor 2>&1); rc=$?
[ $rc -ne 0 ] || fail "restore succeeded with segment ${victim#$W/store/} missing"
echo "$out" | grep -q "log incomplete" || fail "no clear error: $(echo "$out" | tail -2)"
log "note: $(echo "$out" | grep -o 'log incomplete.*' | head -1 | cut -c1-140)"
# put it back: the same restore now works
mv "$W/victim" "$victim"
r2=$(DISK_KEYS="a-d0" write_config r2 1 2)
start_server "$r2"
