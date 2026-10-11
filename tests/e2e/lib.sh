# Shared helpers for blockgw end-to-end cases. Sourced by run.sh inside a per-case subshell.
# Every helper assumes: $W (case work dir), $CASE (case name), $PREFIX (object prefix for this case),
# $BLOCKGW (binary), $S3_ENDPOINT/$S3_BUCKET/$S3_ACCESS_KEY/$S3_SECRET_KEY, $ID_BASE (first ublk id).

set -uo pipefail

log() { echo "    # $*"; }
fail() { echo "    # FAIL: $*"; exit 1; }
skip() { echo "    # SKIP: $*"; exit 77; }
assert_eq() { [ "$1" = "$2" ] || fail "${3:-values differ}: '$1' != '$2'"; }
assert_lt() { [ "$1" -lt "$2" ] || fail "${3:-value too large}: $1 >= $2"; }
assert_gt() { [ "$1" -gt "$2" ] || fail "${3:-value too small}: $1 <= $2"; }

now_ms() { date +%s%3N; }

# write_config NAME DISKS SIZE_GB [POLICY] -> prints the config path. Disk i gets ublk id
# ID_BASE + slot, image dir $W/NAME/d<i>, object prefix $PREFIX/<shared-or-own key>.
# DISK_KEYS (optional, space-separated) overrides the per-disk prefix keys so two servers can
# address the same disk. Runs inside $(...), so the slot counter lives in $W/ids, not in a variable.
write_config() {
  local name=$1 n=$2 size=$3 policy=${4:-local} devs="" i key id
  local cfg=$W/$name.json   # separate statement: $name must be the local one
  local keys=(${DISK_KEYS:-})
  for i in $(seq 0 $((n - 1))); do
    key=${keys[$i]:-$name-d$i}
    id=$((ID_BASE + $(wc -l < "$W/ids")))
    echo "$id" >> "$W/ids"
    devs+="${devs:+,}{\"id\":$id,\"size_gb\":$size,\"dir\":\"$W/$name/d$i\",\"prefix\":\"$PREFIX/$key\",\"policy\":\"$policy\"}"
  done
  local store
  if [ "${E2E_STORE:-s3}" = dir ]; then
    store="\"local_store\":\"$W/store\""   # shared by every server of this case, like a bucket
  else
    store="\"s3\":{\"endpoint\":\"$S3_ENDPOINT\",\"region\":\"${S3_REGION:-us-east-1}\",\"bucket\":\"$S3_BUCKET\",\"access_key\":\"$S3_ACCESS_KEY\",\"secret_key\":\"$S3_SECRET_KEY\",\"path_style\":true}"
  fi
  local extra=""
  [ -n "${METRICS_ADDR:-}" ] && extra=",\"metrics_addr\":\"$METRICS_ADDR\""
  cat > "$cfg" <<EOF
{$store$extra,
 "control_socket":"$W/$name.sock","upload_workers":8,"devices":[$devs]}
EOF
  echo "$cfg"
}

cfg_ids() { python3 -c "import json,sys;print(' '.join(str(d['id']) for d in json.load(open(sys.argv[1]))['devices']))" "$1"; }
cfg_sock() { python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['control_socket'])" "$1"; }

# start_server CONFIG [serve flags]: waits until every disk of CONFIG reports ready (or fails).
start_server() {
  local cfg=$1; shift
  local logf="${cfg%.json}.log" id t0
  echo "=== start $* $(date +%T.%N)" >> "$logf"
  MALLOC_ARENA_MAX=2 setsid "$BLOCKGW" serve "$cfg" "$@" >> "$logf" 2>&1 < /dev/null &
  echo $! > "${cfg%.json}.pid"; echo $! >> "$W/pids"
  t0=$(now_ms)
  for id in $(cfg_ids "$cfg"); do
    until [ "$(sed -n '/^=== start/h; /^=== start/!H; ${x;p}' "$logf" | grep -c "/dev/ublkb$id ready")" -ge 1 ]; do
      kill -0 "$(cat "${cfg%.json}.pid")" 2>/dev/null || { tail -5 "$logf"; fail "server for $cfg exited"; }
      [ $(( $(now_ms) - t0 )) -lt 120000 ] || fail "disk $id not ready after 120 s"
      sleep 0.05
    done
  done
}
server_log() { cat "${1%.json}.log"; }
# last_start_log CONFIG: log lines of the most recent start only
last_start_log() { sed -n '/^=== start/h; /^=== start/!H; ${x;p}' "${1%.json}.log"; }
# The recorded pid is the supervisor (`serve`), or the worker itself with --no-supervisor.
worker_pid() { local p; p=$(cat "${1%.json}.pid"); pgrep -P "$p" -f "blockgw worker" || echo "$p"; }
ready_count() { grep -c "ready" "${1%.json}.log"; }
# kill9_worker CONFIG: crash only the worker; the supervisor restarts it. Waits for every disk to be
# served again (a new "ready" line per disk).
kill9_worker() {
  local cfg=$1 before n t0; before=$(ready_count "$cfg"); n=$(cfg_ids "$cfg" | wc -w)
  kill -9 "$(worker_pid "$cfg")"; t0=$(now_ms)
  until [ "$(ready_count "$cfg")" -ge $((before + n)) ]; do
    [ $(( $(now_ms) - t0 )) -lt 120000 ] || fail "worker not restarted after 120 s"
    sleep 0.05
  done
}
# kill9_all CONFIG: crash the whole process tree (supervisor and worker), as a host-level failure of
# the service would; restart it with start_server.
kill9_all() { local w; w=$(worker_pid "$1"); kill -9 "$(cat "${1%.json}.pid")" "$w" 2>/dev/null; sleep 0.3; }
ctl() { local cfg=$1; shift; "$BLOCKGW" ctl "$(cfg_sock "$cfg")" "$@"; }
# stat CONFIG ID FIELD -> value of FIELD=... on the disk line of `ctl stats`
stat() { ctl "$1" stats | awk -v id="$2" -v f="$3" '$1=="disk" && $2==id { for (i=3;i<=NF;i++) { split($i,a,"="); if (a[1]==f) print a[2] } }'; }
rss_kb() { ctl "$1" stats | awk '$1=="rss_kb"{print $2}'; }
drain() { ctl "$1" drain "$2" > /dev/null; }
image() { echo "$W/$1/d$2/data"; }   # image NAME DISK_INDEX

mkfs_mount() { mkfs.xfs -q -f "/dev/ublkb$1" && mkdir -p "$2" && mount -o noatime,nouuid "/dev/ublkb$1" "$2" && echo "$2" >> "$W/mounts"; }
remount() { mkdir -p "$2" && mount -o noatime,nouuid "/dev/ublkb$1" "$2" && echo "$2" >> "$W/mounts"; }

# gen_tree DIR N SEED: N small files (0.5-16 KiB) of deterministic content in nested dirs
gen_tree() {
  python3 - "$@" <<'PY'
import os, random, sys
d, n, seed = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]); r = random.Random(seed)
for i in range(n):
    p = os.path.join(d, f"d{i % 37}", f"e{i % 11}"); os.makedirs(p, exist_ok=True)
    with open(os.path.join(p, f"f{i}.txt"), "wb") as f: f.write(r.randbytes(r.randint(512, 16384)))
PY
}
# tree_hash DIR: one hash over relative paths and contents
tree_hash() { (cd "$1" && find . -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | cut -c1-16); }
rand_file() { head -c "$2" /dev/urandom > "$1"; }   # rand_file PATH BYTES
file_hash() { sha256sum "$1" | cut -c1-16; }
kernel_errors() { dmesg 2>/dev/null | grep -ciE "ublk.*(error|fail)|XFS \(ublkb.*(shut|error|corrupt)" || true; }
mark_dmesg() { dmesg -C 2>/dev/null || true; }

# cleanup: unmount, stop servers, delete devices. Safe to run repeatedly.
cleanup() {
  local m p id
  [ -f "$W/mounts" ] && tac "$W/mounts" | while read -r m; do umount "$m" 2>/dev/null || umount -l "$m" 2>/dev/null; done
  [ -f "$W/ids" ] && for id in $(sort -u "$W/ids"); do [ -e "/sys/class/ublk-char/ublkc$id" ] && timeout 20 "$BLOCKGW" del "$id" >/dev/null 2>&1; done
  # deleting its devices makes a server return from main; give it time to exit cleanly (an
  # instrumented build writes its coverage profile at exit), then kill whatever is left
  if [ -f "$W/pids" ]; then
    for _ in $(seq 1 100); do
      local alive=0; for p in $(cat "$W/pids"); do kill -0 "$p" 2>/dev/null && alive=1; done
      [ $alive = 0 ] && break; sleep 0.1
    done
    for p in $(cat "$W/pids"); do kill -9 "$p" 2>/dev/null; done
  fi
  [ -f "$W/ids" ] && for id in $(sort -u "$W/ids"); do [ -e "/sys/class/ublk-char/ublkc$id" ] && timeout 20 "$BLOCKGW" del "$id" >/dev/null 2>&1; done
  true
}
