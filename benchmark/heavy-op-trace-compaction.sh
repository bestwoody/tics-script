table="$1"
sec="$2"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-to-be-trace [interval-sec(30)] [log-file(compaction-{db}-{table}.log)]" >&2
	exit 1
fi

if [ -z "$sec" ]; then
	sec="30"
fi

source ./_env.sh

log="./compaction-${storage_db}-${table}.log"

for ((i = 0; i < 999999999; i++)); do
	echo "collect at: `date +%s`" >> "$log"
	./analyze-table-compaction.sh "$table" >> "$log" 2>&1
	echo "" >> "$log"
	sleep $sec
done
