# Checkpoints: after many overwrites of the same region, a restore loads the newest checkpoint and
# fetches only the live data with ranged GETs instead of replaying every segment.
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
DISK_KEYS="a-d0" r=$(write_config r 1 2)
start_server "$r"
line=$(last_start_log "$r" | grep -oE 'restored from the object store.*')
log "metric: uploaded $((raw >> 20)) MiB in total; restore: $line"
echo "$line" | grep -q "/ckpt/" || fail "restore did not use a checkpoint"
live=$(echo "$line" | grep -oE '[0-9]+ MiB of live data' | grep -oE '^[0-9]+')
assert_lt "$live" 12 "live data fetched (8 MiB written last, out of $((raw >> 20)) MiB uploaded)"
assert_eq "$(file_hash "$(image r 0)")" "$(file_hash "$(image a 0)")" "restored image"
