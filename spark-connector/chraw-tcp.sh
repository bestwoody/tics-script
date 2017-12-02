query="$1"
host="127.0.0.1"
port="9006"

set -eu

java \
	-cp ch-sdk/target/*:ch-sdk/target/lib/* \
	org.apache.spark.sql.ch/CHRaw \
	"$query" "$host" "$port" \
