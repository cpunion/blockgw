#!/bin/bash
# blockgw benchmark: the same workload on ext4 (root filesystem), XFS on a kernel loop device, and
# XFS on a blockgw disk, plus durability-policy, multi-disk, idle, crash and restore measurements.
# Run as root on a disposable Linux host (it creates and deletes ublk and loop devices).
#
#   S3_CONFIG=s3.json sudo -E bench/bench.sh            # all phases
#   PHASES="SINGLE POLICY" S3_CONFIG=s3.json sudo -E bench/bench.sh
#
# S3_CONFIG: JSON {"endpoint","region","bucket","access_key","secret_key","path_style"}.
# BLOCKGW: binary (default target/release/blockgw). BENCH_DIR: scratch dir (default /var/tmp/blockgw-bench).
# MIRROR: bare git repository for the clone workload (default: shallow clone of microsoft/TypeScript, ~67k files).
# Phases: KERNEL SINGLE POLICY MULTI IDLE KILL RESTORE. SKIP_BASELINE=1 skips ext4/loop in SINGLE.
# Results: $BENCH_DIR/results/bench.tsv
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
BLOCKGW=$(realpath "${BLOCKGW:-$HERE/../target/release/blockgw}")
S3_CONFIG=$(realpath "${S3_CONFIG:?set S3_CONFIG to an S3 credentials JSON file}")
B=${BENCH_DIR:-/var/tmp/blockgw-bench}; mkdir -p "$B"; cd "$B"
MIRROR=${MIRROR:-$B/mirror.git}
[ -d "$MIRROR" ] || git clone -q --bare --depth 1 https://github.com/microsoft/TypeScript "$MIRROR"
SMALL=$B/small.git
if [ ! -d "$SMALL" ]; then   # ~5k-file subtree of the mirror for the multi-disk phase
  rm -rf "$B/tmp-small" "$B/tmp-src"; git clone -q "$MIRROR" "$B/tmp-src"
  sub=$(cd "$B/tmp-src" && git ls-files | awk -F/ 'NF>2{print $1"/"$2}' | sort | uniq -c | awk '{d=$1-5000; if(d<0)d=-d; print d, $2}' | sort -n | head -1 | cut -d" " -f2)
  mkdir -p "$B/tmp-small" && (cd "$B/tmp-src" && git archive HEAD "$sub" | tar x -C "$B/tmp-small")
  (cd "$B/tmp-small" && git init -q && git add -A && git -c user.email=b@x -c user.name=b commit -qm small) && git clone -q --bare "$B/tmp-small" "$SMALL"
  rm -rf "$B/tmp-small" "$B/tmp-src"
fi
R=$B/results; mkdir -p $R; T=$R/bench.tsv; [ -f $T ] || echo -e "phase\tcase\tmetric\tvalue" > $T
rec(){ printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" | tee -a $T; }
now(){ date +%s%N; }
ms(){ echo $(( ($(now) - $1) / 1000000 )); }
drop(){ sync; echo 3 > /proc/sys/vm/drop_caches; }
CTL="$BLOCKGW ctl /run/blockgw-bench.sock"
mkcfg(){ # mkcfg FIRST_ID N SIZE_GB [policy]
  python3 - "$1" "$2" "$3" "${4:-local}" "$S3_CONFIG" "$B" <<'PY'
import json, sys
first, n, size, pol = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
s3 = json.load(open(sys.argv[5]))
run = __import__("time").strftime("%H%M%S")
devs = [{"id": first + i, "size_gb": size, "dir": f"{sys.argv[6]}/disks/d{first+i}", "prefix": f"run-{run}/d{first+i}", "policy": pol} for i in range(n)]
json.dump({"s3": s3, "control_socket": "/run/blockgw-bench.sock", "devices": devs}, open(f"{sys.argv[6]}/c.json", "w"))
PY
}
serve(){ (MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-2} setsid nohup "$BLOCKGW" serve "$B/c.json" "$@" >> $R/serve.log 2>&1 &); }
wait_dev(){ for i in $(seq 1 600); do [ -b /dev/ublkb$1 ] && return 0; sleep 0.05; done; return 1; }
stop_all(){ for m in $(grep -oE " /m[0-9]+ " /proc/mounts); do umount $m 2>/dev/null; done
  for d in /dev/ublkb*; do [ -b "$d" ] && $BLOCKGW del ${d#/dev/ublkb} >/dev/null 2>&1; done; sleep 1; pkill -f "^$BLOCKGW serve"; sleep 1; rm -rf $B/disks; }
fsync_lat(){ python3 - "$1" "${2:-50}" <<'PY'
import os, sys, time
d, n = sys.argv[1], int(sys.argv[2]); v = []
for i in range(n):
    fd = os.open(f"{d}/fs{i}", os.O_CREAT | os.O_WRONLY); os.write(fd, os.urandom(4096)); t = time.time(); os.fsync(fd); v.append((time.time() - t) * 1000); os.close(fd)
v.sort(); print(f"median={v[len(v)//2]:.1f}ms p90={v[int(len(v)*0.9)]:.1f}ms max={v[-1]:.1f}ms n={n}")
PY
}
workload(){ # workload LABEL DIR : the same steps on any mounted filesystem
  local L=$1 D=$2 t
  rm -rf $D/ts; drop; t=$(now); git clone -q file://$MIRROR $D/ts; rec SINGLE $L clone_67k_s "$(echo "scale=2; $(ms $t)/1000" | bc)"
  t=$(now); sync -f $D; rec SINGLE $L sync_after_clone_ms "$(ms $t)"
  git -C $D/ts status --porcelain >/dev/null; t=$(now); git -C $D/ts status --porcelain >/dev/null; rec SINGLE $L status_hot_ms "$(ms $t)"
  drop; t=$(now); git -C $D/ts status --porcelain >/dev/null; rec SINGLE $L status_cold_ms "$(ms $t)"
  drop; t=$(now); rg -c TODO $D/ts >/dev/null; rec SINGLE $L rg_cold_ms "$(ms $t)"
  t=$(now); rg -c TODO $D/ts >/dev/null; rec SINGLE $L rg_hot_ms "$(ms $t)"
  drop; t=$(now); tar cf - -C $D ts | wc -c >/dev/null; rec SINGLE $L tar_cold_ms "$(ms $t)"
  rec SINGLE $L fsync_4k "$(fsync_lat $D)"
  t=$(now); rm -rf $D/ts $D/fs*; sync -f $D; rec SINGLE $L rm_rf_ms "$(ms $t)"
  fio --name=rw --filename=$D/fio.bin --size=1G --direct=1 --ioengine=io_uring --rw=randwrite --bs=4k --iodepth=32 --runtime=20 --time_based --group_reporting --output-format=json > $R/fio-$L-randwrite.json 2>/dev/null
  rec SINGLE $L fio_4k_randwrite_iops "$(python3 -c "import json;print(int(json.load(open('$R/fio-$L-randwrite.json'))['jobs'][0]['write']['iops']))")"
  fio --name=rr --filename=$D/fio.bin --size=1G --direct=1 --ioengine=io_uring --rw=randread --bs=4k --iodepth=32 --runtime=20 --time_based --group_reporting --output-format=json > $R/fio-$L-randread.json 2>/dev/null
  rec SINGLE $L fio_4k_randread_iops "$(python3 -c "import json;print(int(json.load(open('$R/fio-$L-randread.json'))['jobs'][0]['read']['iops']))")"
  fio --name=sw --filename=$D/fio2.bin --size=2G --direct=1 --ioengine=io_uring --rw=write --bs=1M --iodepth=8 --group_reporting --output-format=json > $R/fio-$L-seqwrite.json 2>/dev/null
  rec SINGLE $L fio_1m_seqwrite_MBps "$(python3 -c "import json;print(int(json.load(open('$R/fio-$L-seqwrite.json'))['jobs'][0]['write']['bw']/1024))")"
  drop; fio --name=sr --filename=$D/fio2.bin --size=2G --direct=1 --ioengine=io_uring --rw=read --bs=1M --iodepth=8 --group_reporting --output-format=json > $R/fio-$L-seqread.json 2>/dev/null
  rec SINGLE $L fio_1m_seqread_MBps "$(python3 -c "import json;print(int(json.load(open('$R/fio-$L-seqread.json'))['jobs'][0]['read']['bw']/1024))")"
  rm -f $D/fio.bin $D/fio2.bin; sync -f $D
}

phase_KERNEL(){
  rec KERNEL host uname "$(uname -r)"; rec KERNEL host cpus_mem "$(nproc) cpu, $(free -m | awk '/Mem/{print $2}') MiB"
  modprobe ublk_drv && rec KERNEL host ublk_drv "loaded; /dev/ublk-control $( [ -c /dev/ublk-control ] && echo present || echo missing)"
  rec KERNEL host config "$(grep -E '^CONFIG_(BLK_DEV_UBLK|IO_URING|DM_CLONE)=' /boot/config-$(uname -r) | tr '\n' ' ')"
  rec KERNEL host root_disk "$(lsblk -dno NAME,SIZE,MODEL | head -1)"
}
phase_SINGLE(){
  stop_all
  if [ -z "${SKIP_BASELINE:-}" ]; then
    mkdir -p /ext4dir; workload ext4 /ext4dir; rm -rf /ext4dir
    truncate -s 20G $B/loop.img; L=$(losetup -f --show $B/loop.img); mkfs.xfs -q -f $L; mkdir -p /mloop; mount -o noatime $L /mloop
    workload loop /mloop; umount /mloop; losetup -d $L; rm -f $B/loop.img
  fi
  mkcfg 0 1 20 local; serve; wait_dev 0; mkfs.xfs -q -f /dev/ublkb0; mkdir -p /m0; mount -o noatime /dev/ublkb0 /m0
  workload blockgw-local /m0
  t=$(now); rec SINGLE blockgw-local drain_after_ms "$($CTL drain 0 | tr -d '\n') (+$(ms $t)ms)"; rec SINGLE blockgw-local stats "$($CTL stats | tr '\n' ' ')"
}
phase_POLICY(){
  mountpoint -q /m0 || { stop_all; mkcfg 0 1 20 local; serve; wait_dev 0; mkfs.xfs -q -f /dev/ublkb0; mkdir -p /m0; mount -o noatime /dev/ublkb0 /m0; }
  rec POLICY local fsync_4k "$(fsync_lat /m0 50)"
  $CTL policy 0 remote >/dev/null; rec POLICY remote fsync_4k "$(fsync_lat /m0 50)"
  rm -rf /m0/ts; t=$(now); git clone -q file://$MIRROR /m0/ts; sync -f /m0; rec POLICY remote clone_67k_plus_sync_s "$(echo "scale=2; $(ms $t)/1000" | bc)"
  # switch while a fsync loop runs: first half local, switch to remote, then back
  $CTL policy 0 local >/dev/null
  python3 - /m0 > $R/policy-switch.txt <<'PY' &
import os, sys, time
d = sys.argv[1]; t0 = time.time()
for i in range(240):
    fd = os.open(f"{d}/sw{i}", os.O_CREAT | os.O_WRONLY); os.write(fd, os.urandom(4096)); t = time.time(); os.fsync(fd)
    print(f"{t - t0:.2f} {(time.time() - t) * 1000:.1f}"); os.close(fd); time.sleep(0.02)
PY
  local p=$!; sleep 2; $CTL policy 0 remote >/dev/null; local ts=$(date +%s.%N); sleep 4; $CTL policy 0 local >/dev/null; local tb=$(date +%s.%N); wait $p
  rec POLICY switch timeline "$(python3 - $R/policy-switch.txt <<'PY'
import sys, statistics as s
rows = [tuple(map(float, l.split())) for l in open(sys.argv[1])]
a = [v for t, v in rows if t < 1.9]; b = [v for t, v in rows if 2.3 < t < 5.8]; c = [v for t, v in rows if t > 6.5]
print(f"local median={s.median(a):.1f}ms -> remote median={s.median(b):.1f}ms -> local median={s.median(c):.1f}ms (n={len(a)}/{len(b)}/{len(c)})")
PY
)"
}
phase_MULTI(){
  stop_all; local N=${MULTI_N:-16}
  # loop baseline: N kernel loop devices, concurrent clone of the small repo
  local devs=() pids="" i
  for i in $(seq 0 $((N-1))); do truncate -s 4G $B/l$i.img; devs[$i]=$(losetup -f --show $B/l$i.img); mkfs.xfs -q -f ${devs[$i]}; mkdir -p /ml$i; mount -o noatime ${devs[$i]} /ml$i; done
  drop; local t=$(now); for i in $(seq 0 $((N-1))); do ( /usr/bin/time -f %e -o /tmp/lt$i git clone -q file://$SMALL /ml$i/r ) & pids="$pids $!"; done; wait $pids; local wall=$(ms $t)
  t=$(now); sync; rec MULTI loop-x$N clone_small "wall=${wall}ms per-disk median=$(sort -n /tmp/lt* | awk '{v[NR]=$1} END{print v[int((NR+1)/2)]}')s sync=$(ms $t)ms"
  for i in $(seq 0 $((N-1))); do umount /ml$i; losetup -d ${devs[$i]}; rm -f $B/l$i.img; done
  mkcfg 100 $N 4 local; serve; for i in $(seq 100 $((100+N-1))); do wait_dev $i; mkfs.xfs -q -f /dev/ublkb$i; mkdir -p /m$i; mount -o noatime /dev/ublkb$i /m$i; done
  rec MULTI blockgw-x$N rss_idle_kb "$($CTL stats | awk '/rss_kb/{print $2}') threads=$(ls /proc/$(pgrep -f "^$BLOCKGW serve")/task | wc -l)"
  drop; pids=""; t=$(now); for i in $(seq 100 $((100+N-1))); do ( /usr/bin/time -f %e -o /tmp/bt$i git clone -q file://$SMALL /m$i/r ) & pids="$pids $!"; done; wait $pids; wall=$(ms $t)
  t=$(now); sync; rec MULTI blockgw-x$N clone_small "wall=${wall}ms per-disk median=$(sort -n /tmp/bt* | awk '{v[NR]=$1} END{print v[int((NR+1)/2)]}')s sync=$(ms $t)ms"
  rec MULTI blockgw-x$N rss_after_kb "$($CTL stats | awk '/rss_kb/{print $2}')"
  t=$(now); for i in $(seq 100 $((100+N-1))); do $CTL drain $i >/dev/null; done; rec MULTI blockgw-x$N drain_all_ms "$(ms $t)"
  sleep 5; rec MULTI blockgw-x$N rss_drained_kb "$($CTL stats | awk '/rss_kb/{print $2}')"
}
phase_IDLE(){
  local a b; a=$($CTL stats | grep -oE "puts=[0-9]+" | awk -F= '{s+=$2} END{print s}'); sleep ${IDLE_S:-120}; b=$($CTL stats | grep -oE "puts=[0-9]+" | awk -F= '{s+=$2} END{print s}')
  rec IDLE blockgw puts_during_idle "$((b-a)) PUTs in ${IDLE_S:-120}s across $($CTL stats | grep -c '^disk') idle mounted disks (no GET/LIST is ever issued after open)"
}
phase_KILL(){
  stop_all; mkcfg 0 1 20 local; serve; wait_dev 0; mkfs.xfs -q -f /dev/ublkb0; mkdir -p /m0; mount -o noatime /dev/ublkb0 /m0; dmesg -C
  dd if=/dev/urandom of=$B/rand.bin bs=1M count=500 status=none
  ( dd if=$B/rand.bin of=/m0/big.bin bs=1M oflag=direct status=none; echo $? > /tmp/dd.rc ) &
  local dp=$!; sleep 2; local before; before=$($CTL stats | grep "disk 0" | grep -oE "uploaded_seq=[0-9]+ queued_bytes=[0-9]+ open_bytes=[0-9]+")
  local t=$(now); kill -9 $(pgrep -f "^$BLOCKGW serve"); : > $R/serve.log; serve --recover; for i in $(seq 1 1200); do grep -q "ublkb0 ready" $R/serve.log 2>/dev/null && break; sleep 0.05; done
  local rt=$(ms $t); wait $dp
  rec KILL dd500 recover "killed during O_DIRECT write ($before); new process serving after ${rt}ms; dd rc=$(cat /tmp/dd.rc); $(grep -E 'dirty regions|recovery' $R/serve.log | sed 's/^disk 0: //' | tr '\n' ';')"
  cmp -s $B/rand.bin /m0/big.bin && rec KILL dd500 content identical || rec KILL dd500 content DIFFERS
  rec KILL dd500 kernel_errors "$(dmesg | grep -ciE 'error|shutdown')"
  rm -rf /m0/ts; ( git clone -q file://$MIRROR /m0/ts; echo $? > /tmp/cl.rc ) & local cp=$!; sleep 3
  t=$(now); kill -9 $(pgrep -f "^$BLOCKGW serve"); serve --recover; sleep 0.5; wait $cp; rec KILL clone recover "clone rc=$(cat /tmp/cl.rc) after kill -9 mid-clone; fsck rc=$(git -C /m0/ts fsck --no-progress >/dev/null 2>&1; echo $?); kernel errors $(dmesg | grep -ciE 'error|shutdown')"
}
phase_RESTORE(){
  umount /m0; $CTL drain 0 >/dev/null; local a; a=$(sha256sum $B/disks/d0/data | cut -c1-16)   # umount writes too: drain after it
  local prefix; prefix=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['devices'][0]['prefix'])" "$B/c.json")
  python3 - "$prefix" "$B/c.json" "$B" <<'PY'
import json, sys
c = json.load(open(sys.argv[2])); c["control_socket"] = "/run/blockgw-bench-r.sock"
c["devices"] = [{"id": 50, "size_gb": c["devices"][0]["size_gb"], "dir": sys.argv[3] + "/disks/restore", "prefix": sys.argv[1]}]
json.dump(c, open(sys.argv[3] + "/r.json", "w"))
PY
  rm -rf $B/disks/restore; : > $R/restore.log; local t=$(now); (setsid nohup "$BLOCKGW" serve "$B/r.json" >> $R/restore.log 2>&1 &); for i in $(seq 1 3000); do grep -qE "ublkb50 ready|Error" $R/restore.log && break; sleep 0.1; done
  rec RESTORE fresh-host restore "$(ms $t)ms; $(grep 'disk 50: epoch' $R/restore.log | tail -1)"
  local b; b=$(sha256sum $B/disks/restore/data | cut -c1-16); rec RESTORE fresh-host image_sha "local=$a restored=$b $( [ "$a" = "$b" ] && echo IDENTICAL || echo DIFFERENT)"
  mkdir -p /mr; mount -o noatime,nouuid /dev/ublkb50 /mr && rec RESTORE fresh-host mount_fsck "files=$(git -C /mr/ts ls-files | wc -l) fsck rc=$(git -C /mr/ts fsck --no-progress >/dev/null 2>&1; echo $?)"
  mount -o noatime,nouuid /dev/ublkb0 /m0; echo x > /m0/after.txt; sync -f /m0 2>/dev/null; local src=$?; sleep 1
  rec RESTORE old-writer fenced "sync rc=$src; $($CTL stats | grep 'disk 0' | grep -oE 'fenced=[a-z]+')"
}
for ph in ${PHASES:-KERNEL SINGLE POLICY MULTI IDLE KILL RESTORE}; do echo "=== $ph $(date -u +%T)"; phase_$ph; done
echo "=== done $(date -u +%T)"
