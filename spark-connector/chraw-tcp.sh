query="$1"
host="127.0.0.1"
port="9006"

set -eu

if [ -z "$query" ]; then
	echo "usage: <bin> $query [ch-host] [ch-port]" >&2
	exit 1
fi

java \
	-cp ch-sdk/target/*:ch-sdk/target/lib/* \
	org.apache.spark.sql.ch/CHRaw \
	"$query" "$host" "$port" \
	2>&1 | grep -v 'SLF4J'
