# A second server opening the same disk fences the first: the old writer's next upload collides,
# its I/O fails, and none of its later writes appear in the object store.
cfg=$(write_config a 1 4)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
gen_tree "$W/m/before" 500 5; sync -f "$W/m"; drain "$cfg" "$id"
hb=$(tree_hash "$W/m/before")
r=$(DISK_KEYS="a-d0" write_config r 1 4)
start_server "$r"
# old writer keeps writing; with the local policy fsync succeeds locally, the upload then collides
gen_tree "$W/m/after" 200 6; sync -f "$W/m" 2>/dev/null; sleep 2
echo x > "$W/m/late.txt" 2>/dev/null; sync -f "$W/m" 2>/dev/null; sleep 1
assert_eq "$(stat "$cfg" "$id" fenced)" true "old writer fenced"
if dd if=/dev/zero of="/dev/ublkb$id" bs=4k count=1 oflag=direct,sync status=none 2>/dev/null; then fail "write to a fenced disk succeeded"; fi
grep -q FENCED "${cfg%.json}.log" || fail "no FENCED log line"
remount "$(cfg_ids "$r")" "$W/r"
assert_eq "$(tree_hash "$W/r/before")" "$hb" "new writer sees everything committed before the takeover"
[ -e "$W/r/late.txt" ] && fail "a write made after the takeover reached the new writer"
true
