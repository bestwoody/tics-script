count=5

set -eu

source ./_env.sh

log="parquet-test.log"
rm -f "$log"

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 21; j++ )); do

		if [ $j -eq 15 ] || [ $j -eq 17 ] || [ $j -eq 18 ]  || [ $j -eq 20 ] || [ $j -eq 21 ]; then
			continue
		fi

		# ./clear-page-cache.sh

		echo "## Running tpch query #"$j", parquet" >>$log
		./tpch-parquet-q.sh $j >>$log 2>&1
		echo >>$log

	done
done
