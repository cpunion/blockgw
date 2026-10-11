# When the worker cannot come back (here: a failpoint makes every restart exit while opening the
# disk), the supervisor fails the device after the orphan timeout: I/O returns EIO instead of
# hanging, the device is deleted and the supervisor exits.
export BLOCKGW_FAILPOINTS_FILE=$W/failpoints; : > "$BLOCKGW_FAILPOINTS_FILE"
cfg=$(write_config a 1 4)
start_server "$cfg" --orphan-timeout 3
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
echo hello > "$W/m/f"; sync -f "$W/m"
echo "open=exit" > "$BLOCKGW_FAILPOINTS_FILE"
sup=$(cat "${cfg%.json}.pid")
kill -9 "$(worker_pid "$cfg")"
t0=$(now_ms)
timeout 60 dd if=/dev/zero of="$W/m/g" bs=4k count=1 oflag=direct,sync status=none 2>/dev/null; rc=$?
ms=$(( $(now_ms) - t0 ))
log "metric: I/O failed after $ms ms (orphan timeout 3 s), dd rc=$rc"
[ $rc -ne 0 ] || fail "write succeeded although no server can run"
[ $rc -ne 124 ] || fail "I/O hung past 60 s"
assert_lt "$ms" 20000 "time until I/O fails"
for _ in $(seq 1 100); do kill -0 "$sup" 2>/dev/null || break; sleep 0.1; done
kill -0 "$sup" 2>/dev/null && fail "supervisor still running"
grep -q "orphan timeout" "${cfg%.json}.log" || fail "no orphan-timeout log line"
[ -e "/sys/block/ublkb$id" ] && fail "block device still present"   # stopped; deleted once unmounted
true
