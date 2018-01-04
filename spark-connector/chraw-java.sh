cmd="$1"
query="$2"
times="$3"
host="127.0.0.1"

set -eu

if [ -z "$cmd" ]; then
	echo "usage: <bin> cmd" >&2
	exit 1
fi

java -XX:MaxDirectMemorySize=5g \
	-cp ch-sdk/target/*:ch-sdk/target/lib/* \
	org.apache.spark.sql.ch/CHRawJava \
	"$host" "$cmd" "$query" "$times" \
	2>&1 | grep -v 'SLF4J'
