set -e

name="$1"

set -u

if [ -z "$name" ]; then
	echo "usage: <bin> data-name(in running/data)" >&2
	exit 1
fi

bin="build/dbms/src/Server/clickhouse"
schema="running/data/$name.schema"
data="running/data/$name.data"

if [ -f "$schema" ]; then
	"$bin" client --query="`cat $schema`"
fi

if [ -f "$data" ]; then
	cat "$data" | "$bin" client --query="INSERT INTO default.$name FORMAT CSV"
fi
