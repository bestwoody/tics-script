n="$1"
partitionsPerSplit="$2"
log="$3"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitionsPerSplit] [log-file=./tpch.log]" >&2
	exit 1
fi

./tpch-spark-r.sh "$n" "$partitionsPerSplit" "$log"
./tpch-parquet-r.sh "$n" "$log"
