count="$1"
partitions="$2"
decoders="$3"
encoders="$4"
log="$5"

set -eu

if [ -z "$count" ]; then
	count=5
fi

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 22; j++ )); do
		if [ $j -eq 15 ]; then
			continue
		fi
		./tpch-spark-r.sh "$j" "$partitions" "$decoders" "$encoders" "$log"
	done
done
