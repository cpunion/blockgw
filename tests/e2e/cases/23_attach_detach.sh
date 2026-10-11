# Disks attached and detached at runtime (`devices_dir`): attach, refuse to detach while mounted,
# detach and re-attach (local image kept), detach with purge and re-attach (lazy restore from the
# store), re-attach after a worker crash, and the checks against conflicting disks.
cfg=$(write_config a 0 2)   # no static disks
python3 -c "import json,sys; c=json.load(open(sys.argv[1])); c['devices_dir']=sys.argv[2]; json.dump(c,open(sys.argv[1],'w'))" "$cfg" "$W/disks"
start_server "$cfg"
for _ in $(seq 1 100); do [ -S "$(cfg_sock "$cfg")" ] && break; sleep 0.05; done
id=$((ID_BASE + 1)); echo "$id" >> "$W/ids"
disk="{\"id\":$id,\"size_gb\":2,\"dir\":\"$W/a/d0\",\"prefix\":\"$PREFIX/dyn\",\"policy\":\"local\"}"
out=$(ctl "$cfg" attach "$disk"); log "note: $out"
case "$out" in ok*) ;; *) fail "attach: $out" ;; esac
[ -f "$W/disks/$id.json" ] || fail "no disk file in devices_dir"
mkfs_mount "$id" "$W/m"
gen_tree "$W/m/tree" 400 23; ht=$(tree_hash "$W/m/tree")
out=$(ctl "$cfg" detach "$id"); case "$out" in *"in use"*) ;; *) fail "detach while mounted: $out" ;; esac
umount_ok "$W/m"
out=$(ctl "$cfg" detach "$id"); log "note: $out"
case "$out" in ok*) ;; *) fail "detach: $out" ;; esac
for _ in $(seq 1 100); do [ -e "/sys/class/ublk-char/ublkc$id" ] || break; sleep 0.05; done
[ -e "/sys/class/ublk-char/ublkc$id" ] && fail "device still exists after detach"
[ -f "$W/disks/$id.json" ] && fail "disk file left in devices_dir"
# re-attach: the local image is reused
out=$(ctl "$cfg" attach "$disk"); case "$out" in ok*) ;; *) fail "re-attach: $out" ;; esac
grep -q "fresh=false" "${cfg%.json}.log" || fail "re-attach did not reuse the local image"
remount "$id" "$W/m"
assert_eq "$(tree_hash "$W/m/tree")" "$ht" "tree after detach + attach"
umount_ok "$W/m"
# park: detach with purge, then attach restores from the store (lazily)
out=$(ctl "$cfg" detach "$id" purge); log "note: $out"
[ -e "$W/a/d0" ] && fail "purge left the local dir"
out=$(ctl "$cfg" attach "$disk"); log "note: $out"
case "$out" in ok*) ;; *) fail "attach after purge: $out" ;; esac
grep -q "lazy restore" "${cfg%.json}.log" || fail "attach after purge did not restore from the store"
remount "$id" "$W/m"
assert_eq "$(tree_hash "$W/m/tree")" "$ht" "tree after purge + attach"
# a worker crash: the supervisor's new worker re-attaches it from devices_dir
before=$(grep -c "/dev/ublkb$id ready" "${cfg%.json}.log")
kill9_worker "$cfg"
for _ in $(seq 1 200); do [ "$(grep -c "/dev/ublkb$id ready" "${cfg%.json}.log")" -gt "$before" ] && break; sleep 0.05; done
[ "$(grep -c "/dev/ublkb$id ready" "${cfg%.json}.log")" -gt "$before" ] || fail "not re-attached after a worker crash"
assert_eq "$(tree_hash "$W/m/tree")" "$ht" "tree across a worker crash"
umount_ok "$W/m"
# conflicts
dup="{\"id\":$((id + 1)),\"size_gb\":2,\"dir\":\"$W/a/d1\",\"prefix\":\"$PREFIX/dyn\",\"policy\":\"local\"}"
out=$(ctl "$cfg" attach "$dup"); case "$out" in *"share the prefix"*) ;; *) fail "attach with a prefix in use: $out" ;; esac
out=$(ctl "$cfg" attach "$disk"); case "$out" in *"appears twice"*) ;; *) fail "attach of an attached id: $out" ;; esac
out=$(ctl "$cfg" detach 999999); case "$out" in *"no such disk"*) ;; *) fail "detach unknown: $out" ;; esac
