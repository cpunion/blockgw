# One process serves several disks; concurrent writers stay isolated; memory stays small.
cfg=$(write_config a 4 2)
start_server "$cfg"
ids=($(cfg_ids "$cfg"))
for i in 0 1 2 3; do mkfs_mount "${ids[$i]}" "$W/m$i"; done
log "metric: rss idle with 4 disks $(rss_kb "$cfg") KiB"
pids=""; for i in 0 1 2 3; do gen_tree "$W/m$i/t" 1500 $((80 + i)) & pids="$pids $!"; done; wait $pids
sync
for i in 0 1 2 3; do gen_tree "$W/ref$i" 1500 $((80 + i)); assert_eq "$(tree_hash "$W/m$i/t")" "$(tree_hash "$W/ref$i")" "disk $i content"; done
for i in 0 1 2 3; do drain "$cfg" "${ids[$i]}"; done
rss=$(rss_kb "$cfg"); log "metric: rss after load $rss KiB"
assert_lt "$rss" $((200 * 1024)) "server RSS"
