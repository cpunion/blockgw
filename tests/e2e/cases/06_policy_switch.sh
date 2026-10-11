# Durability policy is per disk and switchable at runtime: under `remote`, nothing is queued once
# fsync returns; switching back restores local latency; `lag` updates the limit.
cfg=$(write_config a 1 4 local)
start_server "$cfg"
id=$(cfg_ids "$cfg")
mkfs_mount "$id" "$W/m"
fsync_ms() { python3 - "$W/m" "$1" <<'PY'
import os, sys, time, statistics as st
d, n = sys.argv[1], int(sys.argv[2]); v = []
for i in range(n):
    fd = os.open(f"{d}/p{i}", os.O_CREAT | os.O_WRONLY); os.write(fd, os.urandom(4096)); t = time.time(); os.fsync(fd); v.append((time.time() - t) * 1000); os.close(fd)
print(f"{st.median(v):.2f}")
PY
}
l1=$(fsync_ms 30)
ctl "$cfg" policy "$id" remote | grep -q "policy=remote" || fail "policy switch not acknowledged"
assert_eq "$(stat "$cfg" "$id" policy)" remote
for i in 1 2 3 4 5; do
  head -c 1048576 /dev/urandom > "$W/m/r$i"; python3 -c "import os,sys; fd=os.open(sys.argv[1], os.O_RDONLY); os.fsync(fd)" "$W/m/r$i"
  assert_eq "$(stat "$cfg" "$id" queued_bytes)" 0 "remote: queue empty right after fsync #$i"
done
r1=$(fsync_ms 30)
ctl "$cfg" policy "$id" local >/dev/null; l2=$(fsync_ms 30)
log "metric: fsync median local=${l1}ms remote=${r1}ms local-again=${l2}ms"
ctl "$cfg" lag "$id" 64 | grep -q "max_lag=64MiB" || fail "lag update not acknowledged"
