count="$1"
log="$2"

set -eu

if [ -z "$count" ]; then
	count=10
fi
if [ -z "$log" ]; then
	log="./stable-test.log"
fi

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 22; j++ )); do
		if [ $j -eq 15 ]; then
			continue
		fi
		echo "## Running tpch query #"$j", parquet" >>$log
		./tpch-parquet-q.sh $j >>$log 2>&1
		echo >>$log
		./stable-test-avg-result.sh $log > $log.md
	done
done
