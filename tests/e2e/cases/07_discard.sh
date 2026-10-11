# fstrim frees space in the local image, and discards replay on restore (byte-identical image).
cfg=$(write_config a 1 4)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
rand_file "$W/m/big.bin" $((256 << 20)); sync -f "$W/m"; drain "$cfg" "$id"
before=$(du -k "$(image a 0)" | cut -f1)
rm "$W/m/big.bin"; sync -f "$W/m"
# XFS skips extents that are still "busy" (freed, but the freeing transaction is not yet in a log
# checkpoint), so a trim right after rm can release nothing. Retry until the image shrinks.
for i in $(seq 1 10); do
  fstrim "$W/m"; sync -f "$W/m"
  after=$(du -k "$(image a 0)" | cut -f1)
  [ "$after" -lt $((before - 200000)) ] && break
  sleep 1
done
umount "$W/m"; drain "$cfg" "$id"
after=$(du -k "$(image a 0)" | cut -f1)
log "metric: image allocation ${before} KiB -> ${after} KiB after rm + fstrim"
assert_lt "$after" $((before - 200000)) "fstrim released the file's blocks in the image"
DISK_KEYS="a-d0" r=$(write_config r 1 4)
start_server "$r"
assert_eq "$(file_hash "$(image r 0)")" "$(file_hash "$(image a 0)")" "restored image after discards"
