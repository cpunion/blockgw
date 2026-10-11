#!/bin/bash
# blockgw end-to-end test runner (TAP output). Needs root, the ublk_drv module, xfsprogs, python3,
# and an S3-compatible endpoint (a local SeaweedFS container is started when S3_ENDPOINT is unset).
#
#   sudo tests/e2e/run.sh                 # all cases
#   sudo tests/e2e/run.sh 03 05           # cases whose file name starts with 03 or 05
#
# Environment: BLOCKGW (binary, default target/release/blockgw), S3_ENDPOINT, S3_BUCKET,
# S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION, E2E_STORE (s3 | dir), E2E_WORK (scratch dir), ID_BASE (first ublk id, default 200),
# KEEP_S3=1 (leave the SeaweedFS container running).
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
export BLOCKGW=${BLOCKGW:-$ROOT/target/release/blockgw}
export S3_BUCKET=${S3_BUCKET:-blockgw-e2e} S3_ACCESS_KEY=${S3_ACCESS_KEY:-any} S3_SECRET_KEY=${S3_SECRET_KEY:-any}
export ID_BASE=${ID_BASE:-200}
WORK=${E2E_WORK:-/tmp/blockgw-e2e}

[ "$(id -u)" = 0 ] || { echo "run as root (ublk and mount need it)"; exit 2; }
[ -x "$BLOCKGW" ] || { echo "binary not found: $BLOCKGW (cargo build --release)"; exit 2; }
[ -c /dev/ublk-control ] || modprobe ublk_drv 2>/dev/null
[ -c /dev/ublk-control ] || { echo "ublk is not available (modprobe ublk_drv failed)"; exit 2; }
for t in mkfs.xfs python3 sha256sum fstrim; do command -v $t >/dev/null || { echo "missing tool: $t"; exit 2; }; done

started_s3=""
export E2E_STORE=${E2E_STORE:-s3}   # s3 (SeaweedFS or $S3_ENDPOINT) or dir (local_store in each case's work dir)
if [ "$E2E_STORE" = dir ]; then
  :
elif [ -z "${S3_ENDPOINT:-}" ]; then
  command -v docker >/dev/null || { echo "set S3_ENDPOINT or install docker"; exit 2; }
  docker rm -f blockgw-e2e-s3 >/dev/null 2>&1
  docker run -d --name blockgw-e2e-s3 -p 127.0.0.1:8333:8333 chrislusf/seaweedfs:4.47 server -s3 -dir=/data >/dev/null || exit 2
  started_s3=1
  export S3_ENDPOINT=http://127.0.0.1:8333
  for i in $(seq 1 60); do curl -s -o /dev/null http://127.0.0.1:8333/ && break; sleep 1; done
fi
[ "$E2E_STORE" = dir ] || curl -s -o /dev/null -X PUT "$S3_ENDPOINT/$S3_BUCKET" || true   # anonymous bucket create (SeaweedFS default)

mapfile -t CASES < <(ls "$HERE"/cases/*.sh | sort)
if [ $# -gt 0 ]; then
  sel=(); for c in "${CASES[@]}"; do for p in "$@"; do [[ $(basename "$c") == $p* ]] && sel+=("$c"); done; done; CASES=("${sel[@]}")
fi

RUN=$(date +%Y%m%d-%H%M%S)-$$
echo "TAP version 13"; echo "# store: $E2E_STORE"; echo "1..${#CASES[@]}"
n=0; failed=0; slot=0
for c in "${CASES[@]}"; do
  n=$((n + 1)); name=$(basename "$c" .sh)
  W=$WORK/$RUN/$name; mkdir -p "$W"; : > "$W/ids"; : > "$W/pids"; : > "$W/mounts"
  t0=$(date +%s%3N)
  (
    export W CASE=$name PREFIX=e2e/$RUN/$name ID_BASE=$((ID_BASE + slot))
    source "$HERE/lib.sh"
    trap cleanup EXIT
    source "$c"
  ) > "$W/out.txt" 2>&1
  rc=$?
  ms=$(( $(date +%s%3N) - t0 ))
  slot=$((slot + 16))
  case $rc in
    0) echo "ok $n - $name (${ms} ms)" ;;
    77) echo "ok $n - $name # SKIP $(grep -m1 'SKIP:' "$W/out.txt" | sed 's/.*SKIP: //')" ;;
    *) echo "not ok $n - $name (${ms} ms)"; failed=$((failed + 1)); sed 's/^/    /' "$W/out.txt" | tail -40 ;;
  esac
  grep -E '^    # (metric|note):' "$W/out.txt" || true
done
[ -n "$started_s3" ] && [ -z "${KEEP_S3:-}" ] && docker rm -f blockgw-e2e-s3 >/dev/null 2>&1
echo "# $((n - failed))/$n passed; logs in $WORK/$RUN"
[ $failed -eq 0 ]
