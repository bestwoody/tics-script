count=5

set -eu

source ./_env.sh

log="parquet-test.log"
rm -f "$log"

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 21; j++ )); do
		# ./clear-page-cache.sh
		echo "## Running tpch query #"$j", parquet" >>$log
		./tpch-parquet-q.sh $j >>$log 2>&1
		echo >>$log
	done
done
