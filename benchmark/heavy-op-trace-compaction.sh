table="$1"
sec="$2"
log="$3"

set -eu

source ./_env.sh

if [ -z "$table" ]; then
	echo "usage: <bin> table-to-be-trace [interval-sec(15)] [log-file(compaction-{db}-{table}.log)]" >&2
	exit 1
fi

if [ -z "$sec" ]; then
	sec="15"
fi

if [ -z "$log" ]; then
	log="./compaction-${storage_db}-${table}.log"
fi

source ./_env.sh

for ((i = 0; i < 999999999; i++)); do
	echo "Collect at: `date +%s`" >> "$log"
	./analyze-table-compaction.sh "$table" >> "$log" 2>&1
	echo "" >> "$log"
	sleep $sec
done
