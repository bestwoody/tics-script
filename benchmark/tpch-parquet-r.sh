n="$1"
log="$5"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [log-file=./tpch.log]" >&2
	exit 1
fi

if [ -z $log ]; then
	log="./tpch.log"
fi

./tpch-parquet-q.sh "$n" >> "$log"
./gen-tpch-report.sh "$log" > "$log.md"
