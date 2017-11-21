set -e

name="$1"

set -u

if [ -z "$name" ]; then
	echo "usage: <bin> data-name(in running/data)" >&2
	exit 1
fi

bin="build/dbms/src/Server/clickhouse"

gen=""
data="running/data/$name.data"
if [ -f "$data" ]; then
	gen="cat $data"
fi

if [ -z "$gen" ]; then
	gen="running/data/$name.sh"
	if [ ! -f "$gen" ]; then
		gen=""
	else
		gen="bash $gen"
	fi
fi

if [ -z "$gen" ]; then
	gen="running/data/$name.py"
	if [ ! -f "$gen" ]; then
		gen=""
	else
		gen="python $gen"
	fi
fi

if [ -z "$gen" ]; then
	echo "no data found, exit" >&2
	exit 1
fi

schema="running/data/$name.schema"
if [ -f "$schema" ]; then
	DYLD_LIBRARY_PATH="" "$bin" client --query="`cat $schema`"
fi

$gen | DYLD_LIBRARY_PATH="" "$bin" client --query="INSERT INTO $name FORMAT CSV"
