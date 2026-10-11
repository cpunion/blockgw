# Operability: Prometheus metrics over HTTP and over the control socket, JSON stats, config checks.
port=$((19000 + ID_BASE % 1000))
cfg=$(METRICS_ADDR=127.0.0.1:$port write_config a 1 2)
start_server "$cfg"
id=$(cfg_ids "$cfg")
m() { curl -sf "http://127.0.0.1:$port/metrics"; }
val() { m | awk -v k="$1{disk=\"$id\"}" '$1==k {print $2}'; }
# capture first: `m | grep -q` would close the pipe early, and pipefail turns curl's EPIPE into a failure
out=$(m) || fail "metrics endpoint"
grep -q '^blockgw_info{version="' <<< "$out" || fail "no blockgw_info"
for f in blockgw_committed_segments blockgw_upload_lag_bytes blockgw_puts_total blockgw_upload_errors_total blockgw_flush_wait_seconds_total blockgw_fenced blockgw_policy_remote; do
  grep -q "^# TYPE $f " <<< "$out" || fail "missing metric family $f"
done
p0=$(val blockgw_puts_total)
mkfs_mount "$id" "$W/m"
rand_file "$W/m/f" $((8 << 20)); sync -f "$W/m"; drain "$cfg" "$id"
p1=$(val blockgw_puts_total)
assert_gt "$p1" "$p0" "puts_total grows with writes"
assert_eq "$(val blockgw_upload_lag_bytes)" 0 "no lag after drain"
assert_eq "$(val blockgw_fenced)" 0
assert_gt "$(m | awk '$1=="blockgw_process_resident_memory_bytes"{print $2}')" 1000000 "rss reported"
curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$port/other" | grep -q 404 || fail "unknown path should 404"
grep -q "^blockgw_puts_total{disk=\"$id\"} $p1" <<< "$(ctl "$cfg" metrics)" || fail "ctl metrics disagrees with HTTP"
ctl "$cfg" stats json | python3 -c "import json,sys; s=json.load(sys.stdin); d=s['disks'][0]; assert d['id']==$id and d['policy']=='local' and d['puts']>0, s" || fail "stats json"
"$BLOCKGW" check-config "$cfg" | grep -q "^ok: 1 disk" || fail "check-config on a valid config"
python3 -c "import json,sys; c=json.load(open(sys.argv[1])); c['devices'].append(dict(c['devices'][0])); json.dump(c, open(sys.argv[2],'w'))" "$cfg" "$W/dup.json"
"$BLOCKGW" check-config "$W/dup.json" >/dev/null 2>&1 && fail "duplicate device id accepted"
"$BLOCKGW" --version | grep -q "^blockgw " || fail "--version"
grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z INFO disk ' "${cfg%.json}.log" || fail "log lines are timestamped and leveled"
true
