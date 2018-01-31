count=1000
partitions=16
encoders=1
decoders=16

set -eu

source ./_env.sh

log="stable-test.log"
rm -f "$log"

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 21; j++ )); do

		if [ $j -eq 15 ] || [ $j -eq 17 ] || [ $j -eq 18 ] || [ $j -eq 21 ]; then
			continue
		fi

		# ./clear-page-cache.sh

		echo "## Running tpch query #"$j", partitions=$partitions, decoders=$decoders, encoders=$encoders, pushdown=$pushdown, codegen=$codegen"  >>$log
		./tpch-spark-q.sh $j $partitions $encoders $decoders >>$log 2>&1
		echo >>$log

	done
done
