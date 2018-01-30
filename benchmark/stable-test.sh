set -eu

log="stable-test.log"
rm -f "$log"

for (( i = 0; i < 1000; i++ )); do
	for (( j =1; j <= 21; j++ )); do

		if [ $j -eq 15 ] || [ $j -eq 17 ] || [ $j -eq 18 ] || [ $j -eq 21 ]; then
			continue
		fi

		# ./clear-page-cache.sh

		echo "## running q $j"  >>$log
		echo ./tpch-spark-q.sh $j 16 1 16 >>$log 2>&1
		./tpch-spark-q.sh $j 16 1 16 >>$log 2>&1

	done
done
