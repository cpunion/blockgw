# After a lazy restore, a worker crash must not drop a write to a block that was never fetched
# while keeping a later write: the presence bitmap is only persisted at flush, so recovery must take
# written data from the image, not from the (stale) bitmap. Uploads are held back by a failpoint so
# both writes are still un-uploaded when the worker dies.
blk() { python3 -c "import sys; sys.stdout.buffer.write(sys.argv[1].encode() * 4096)" "$1" > "$W/$1.blk"; }
put() { dd if="$W/$2.blk" of="$(blkdev $1)" bs=4096 seek="$3" oflag=direct conv=notrunc status=none; }   # put ID PATTERN BLOCK
get() { dd if="$(blkdev $1)" bs=4096 skip="$2" count=1 iflag=direct status=none | head -c1; }          # get ID BLOCK -> first byte
for p in A B a b; do blk $p; done
X=2048; Y=133120   # 8 MiB and 520 MiB: different write-intent regions
cfg=$(write_config a 1 2)
start_server "$cfg"; id=$(cfg_ids "$cfg")
put "$id" A $X; put "$id" B $Y; sync
drain "$cfg" "$id"; kill9_all "$cfg"
export BLOCKGW_FAILPOINTS_FILE=$W/failpoints
echo "upload=error" > "$BLOCKGW_FAILPOINTS_FILE"
r=$(HYDRATE_MBPS=0 DISK_KEYS="a-d0" write_config r 1 2)
start_server "$r"; rid=$(cfg_ids "$r")
assert_eq "$(get "$rid" $Y)" B "Y fetched on demand"
put "$rid" a $X   # X was never fetched: absent
put "$rid" b $Y   # Y is present; this write comes after X's
: > "$BLOCKGW_FAILPOINTS_FILE"
kill9_worker "$r"
assert_eq "$(get "$rid" $X)$(get "$rid" $Y)" ab "both writes survive the crash, in order"
drain "$r" "$rid"; kill9_all "$r"
f=$(DISK_KEYS="a-d0" write_config f 1 2)
start_server "$f"; fid=$(cfg_ids "$f")
assert_eq "$(get "$fid" $X)$(get "$fid" $Y)" ab "and they reached the store"
