query="$1"
partitions="$2"
conc="$3"
host="127.0.0.1"
port="9006"

set -eu

if [ -z "$conc" ]; then
	echo "usage: <bin> query partitions conc" >&2
	exit 1
fi

java -XX:MaxDirectMemorySize=5g \
	-cp chspark/target/*:chspark/target/lib/* \
	org.apache.spark.sql.ch/CHRawScala \
	"$query" "$partitions" "$conc" "$host" "$port" \
	2>&1 | grep -v 'SLF4J'
