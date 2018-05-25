count="$1"
log="$2"

set -eu

if [ -z "$count" ]; then
	count=5
fi

function cmp()
{
	local n="$1"
	./tpch-spark-r.sh "$n" 16 1 16 "$log"
	./tpch-spark-r.sh "$n" 16 2 16 "$log"
	./tpch-spark-r.sh "$n" 16 4 16 "$log"
	./tpch-spark-r.sh "$n" 16 8 16 "$log"
	./tpch-spark-r.sh "$n" 16 16 16 "$log"
	./tpch-spark-r.sh "$n" 16 4 32 "$log"
	./tpch-spark-r.sh "$n" 32 4 16 "$log"
	./tpch-spark-r.sh "$n" 8 4 16 "$log"
	./tpch-spark-r.sh "$n" 4 4 16 "$log"
	./tpch-spark-r.sh "$n" 2 4 16 "$log"
	./tpch-spark-r.sh "$n" 2 8 16 "$log"
	./tpch-spark-r.sh "$n" 1 4 16 "$log"
	./tpch-spark-r.sh "$n" 1 8 16 "$log"
	./tpch-spark-r.sh "$n" 1 16 16 "$log"
}

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 22; j++ )); do
		if [ $j -eq 15 ]; then
			continue
		fi
		if [ $j -eq 17 ] || [ $j -eq 18 ] || [ $j -eq 20 ] || [ $j -eq 21 ]; then
			continue
		fi
		cmp "$j"
	done
done
