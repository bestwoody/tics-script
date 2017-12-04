query="$1"
decode="$2"
host="127.0.0.1"
port="9006"

set -eu

if [ -z "$query" ]; then
	echo "usage: <bin> $query [decode] [ch-host] [ch-port]" >&2
	exit 1
fi

if [ -z "$decode" ]; then
	decode="true"
fi

java \
	-cp ch-sdk/target/*:ch-sdk/target/lib/* \
	org.apache.spark.sql.ch/CHRaw \
	"$query" "$decode" "$host" "$port" \
	2>&1 | grep -v 'SLF4J'
