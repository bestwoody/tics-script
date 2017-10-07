in="$1"
out="data/db"
if [ ! -z "$2" ]; then
	out="$2"
fi

set -eu

if [ -z "$in" ]; then
	echo "usage: <bin> data-source-file output-file" >&2
	exit 1
fi

rm -f "$out"
rm -f "$out.idx"

# bin/analysys index build ?
#
# -align int
#   block size/offset align (default 512)
# -compress string
#   compress method, '' means no compress (default "snappy")
# -conc int
#   conrrent threads, '0' means auto detect
# -gran int
#   index granularity (default 8192)
# -in string
#   input file path (default "origin")
# -out string
#   output path (default "db")
#
#shortcut: <in> <out> <compress> <conc> <gran> <align>

bin/analysys index build \
	-in="$in" \
	-out="$out" \
	-compress="snappy" \
	-conc=0 \
	-gran=8192 \
	-align=512 \
