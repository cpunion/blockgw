# Encryption at rest: with key_file set, nothing readable reaches the store; the right key
# restores the disk; a wrong key or a missing key fails to open it, with a clear error, instead of
# serving wrong data.
key=$W/disk.key; head -c 32 /dev/urandom > "$key"; chmod 600 "$key"
cfg=$(KEY_FILE=$key write_config a 1 2)
start_server "$cfg"; id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
marker="blockgw-plaintext-marker-$RANDOM$RANDOM"
python3 -c "import sys; open(sys.argv[1],'w').write((sys.argv[2] + '\n') * 20000)" "$W/m/marker.txt" "$marker"
gen_tree "$W/m/tree" 300 19
ht=$(tree_hash "$W/m/tree"); hm=$(file_hash "$W/m/marker.txt")
umount_ok "$W/m"; drain "$cfg" "$id"
if [ "${E2E_STORE:-s3}" = dir ]; then
  [ -n "$(ls -R "$W/store")" ] || fail "store is empty"
  grep -rqa "$marker" "$W/store" && fail "plaintext reached the store"
  grep -rqaE "BGS2|BGC2" "$W/store" && fail "segment or checkpoint headers are visible in the store"
  log "note: $(find "$W/store" -type f | wc -l) objects, none with plaintext"
fi
kill9_all "$cfg"
# the right key on a fresh host
r=$(KEY_FILE=$key DISK_KEYS="a-d0" write_config r 1 2)
start_server "$r"; rid=$(cfg_ids "$r")
remount "$rid" "$W/rm" ro,norecovery
assert_eq "$(tree_hash "$W/rm/tree")" "$ht" "tree restored with the key"
assert_eq "$(file_hash "$W/rm/marker.txt")" "$hm" "file restored with the key"
umount_ok "$W/rm"
kill9_all "$r"
# a wrong key, then no key: opening must fail, never serve
expect_open_failure() {   # expect_open_failure CONFIG PATTERN WHAT
  local out; out=$(timeout 60 "$BLOCKGW" serve "$1" --no-supervisor 2>&1); local rc=$?
  [ $rc -ne 0 ] || fail "$3: server started"
  echo "$out" | grep -q "$2" || fail "$3: no clear error: $(echo "$out" | tail -2)"
  [ -e "/dev/ublkb$(cfg_ids "$1")" ] && [ -e "/sys/block/ublkb$(cfg_ids "$1")" ] && fail "$3: device was attached"
  log "note: $3: $(echo "$out" | grep -o "$2.*" | head -1 | cut -c1-120)"
}
other=$W/other.key; head -c 32 /dev/urandom > "$other"; chmod 600 "$other"
w=$(KEY_FILE=$other DISK_KEYS="a-d0" write_config w 1 2)
expect_open_failure "$w" "decryption failed" "wrong key"
n=$(DISK_KEYS="a-d0" write_config n 1 2)
expect_open_failure "$n" "has no key_file" "no key"
# a world-readable key file is refused by check-config
chmod 644 "$key"
"$BLOCKGW" check-config "$r" > /dev/null 2>&1 && fail "check-config accepted a world-readable key file"
chmod 600 "$key"
true
