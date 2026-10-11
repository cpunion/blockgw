# Key rotation: a disk written under key A is opened with key B (A kept as an old, decrypt-only
# key); `ctl rekey` rewrites everything sealed with A, after which B alone restores the disk.
mk() { head -c 32 /dev/urandom > "$W/$1.key"; chmod 600 "$W/$1.key"; }
mk A; mk B
cfg=$(KEY_FILE=$W/A.key write_config a 1 2)
start_server "$cfg"; id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
gen_tree "$W/m/tree" 400 24; rand_file "$W/m/big.bin" $((24 << 20))
umount_ok "$W/m"; drain "$cfg" "$id"; kill9_all "$cfg"
# rotate: B for new writes, A to read the rest
r=$(KEY_FILE=$W/B.key OLD_KEY_FILE=$W/A.key DISK_KEYS="a-d0" write_config r 1 2)
start_server "$r"; rid=$(cfg_ids "$r")
remount "$rid" "$W/m"
ht=$(tree_hash "$W/m/tree"); hb=$(file_hash "$W/m/big.bin")
gen_tree "$W/m/tree2" 100 25   # written under B
umount_ok "$W/m"
# before rekey, B alone cannot open the disk
n=$(KEY_FILE=$W/B.key DISK_KEYS="a-d0" write_config n 1 2)
out=$(timeout 60 "$BLOCKGW" serve "$n" --no-supervisor 2>&1) && fail "B alone opened the disk before rekey"
echo "$out" | grep -q "decryption failed" || fail "no clear error before rekey: $(echo "$out" | tail -1)"
out=$(ctl "$r" rekey "$rid"); log "note: $out"
case "$out" in *"no segment or checkpoint needs an old key"*) ;; *) fail "rekey: $out" ;; esac
hd=$(dev_hash "$rid")
drain "$r" "$rid"; kill9_all "$r"
# B alone, on a fresh host
f=$(KEY_FILE=$W/B.key DISK_KEYS="a-d0" write_config f 1 2)
start_server "$f"; fid=$(cfg_ids "$f")
assert_eq "$(dev_hash "$fid")" "$hd" "whole device restored with the new key alone"
remount "$fid" "$W/f" ro,norecovery
assert_eq "$(tree_hash "$W/f/tree")" "$ht" "tree written under the old key"
assert_eq "$(file_hash "$W/f/big.bin")" "$hb" "big file written under the old key"
umount_ok "$W/f"
