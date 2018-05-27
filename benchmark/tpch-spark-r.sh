n="$1"
partitions="$2"
log="$3"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitions] [log-file=./tpch.log]" >&2
	exit 1
fi

if [ -z $log ]; then
	log="./tpch.log"
fi

./tpch-spark-q.sh "$n" "$partitions" >> "$log"
./tpch-gen-report.sh "$log" > "$log.md"
