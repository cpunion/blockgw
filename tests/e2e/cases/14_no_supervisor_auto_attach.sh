# Without the supervisor, restarting `serve` after a crash re-attaches the existing device on its own
# (no flag needed) and the data is intact.
cfg=$(write_config a 1 4)
start_server "$cfg" --no-supervisor
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
gen_tree "$W/m/t" 1500 14; sync -f "$W/m"; h=$(tree_hash "$W/m/t")
kill -9 "$(cat "${cfg%.json}.pid")"; sleep 0.3
[ -e "/sys/class/ublk-char/ublkc$id" ] || fail "device should survive its server (user recovery)"
start_server "$cfg" --no-supervisor
grep -q "re-attaching the existing device" "${cfg%.json}.log" || fail "did not re-attach"
assert_eq "$(tree_hash "$W/m/t")" "$h" "tree after re-attach"
echo after > "$W/m/after"; sync -f "$W/m" || fail "write after re-attach"
