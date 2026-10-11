# An idle mounted disk makes no object-store requests (no polling): PUT count stays flat.
cfg=$(write_config a 2 2)
start_server "$cfg"
ids=($(cfg_ids "$cfg"))
for i in 0 1; do mkfs_mount "${ids[$i]}" "$W/m$i"; echo hi > "$W/m$i/x"; sync -f "$W/m$i"; drain "$cfg" "${ids[$i]}"; done
sleep 3
p0=$(ctl "$cfg" stats | grep -oE 'puts=[0-9]+' | cut -d= -f2 | paste -sd+ | bc)
sleep ${IDLE_S:-20}
p1=$(ctl "$cfg" stats | grep -oE 'puts=[0-9]+' | cut -d= -f2 | paste -sd+ | bc)
assert_eq "$p1" "$p0" "PUTs while idle"
