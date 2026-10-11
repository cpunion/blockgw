# Checkpoints: after many overwrites of the same region, an eager restore (lazy=false) loads the
# newest checkpoint and fetches only the live data with ranged GETs instead of replaying every segment.
cfg=$(write_config a 1 2)
start_server "$cfg"
id=$(cfg_ids "$cfg")
for i in $(seq 1 20); do
  head -c $((8 << 20)) /dev/urandom > "$W/region.bin"
  dd if="$W/region.bin" of="/dev/ublkb$id" bs=1M oflag=direct,sync status=none conv=notrunc
done
out=$(ctl "$cfg" drain "$id"); log "note: $out"
echo "$out" | grep -qE "checkpointed|checkpoint current" || fail "drain did not checkpoint"
raw=$(stat "$cfg" "$id" raw_bytes)
r=$(LAZY=false DISK_KEYS="a-d0" write_config r 1 2)   # the eager path: fetch every live run at open
start_server "$r"
line=$(last_start_log "$r" | grep -oE 'restored from the object store.*')
log "metric: uploaded $((raw >> 20)) MiB in total; restore: $line"
echo "$line" | grep -q "/ckpt/" || fail "restore did not use a checkpoint"
live=$(echo "$line" | grep -oE '[0-9]+ MiB of live data' | grep -oE '^[0-9]+')
assert_lt "$live" 12 "live data fetched (8 MiB written last, out of $((raw >> 20)) MiB uploaded)"
assert_eq "$(dev_hash "$(cfg_ids "$r")")" "$(file_hash "$(image a 0)")" "restored image"
# an eager restore leaves an explicit presence file: a restart finds every block local
kill9_all "$r"; start_server "$r"
last_start_log "$r" | grep -q "; 0 blocks absent" || fail "after a restart the restored disk has absent blocks: $(last_start_log "$r" | grep -o 'blocks absent.*\|[0-9]* blocks absent' | head -1)"

