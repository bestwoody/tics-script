count="$1"
partitions="$2"
decoders="$3"
encoders="$4"
log="$5"

set -eu

if [ -z "$count" ]; then
	count=10
fi
if [ -z "$partitions" ]; then
	partitions=16
fi
if [ -z "$decoders" ]; then
	decoders=4
fi
if [ -z "$encoders" ]; then
	encoders=16
fi
if [ -z "$log" ]; then
	log="./stable-test.log"
fi

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 22; j++ )); do
		if [ $j -eq 15 ]; then
			continue
		fi
		if [ $j -eq 17 ] || [ $j -eq 18 ] || [ $j -eq 20 ] || [ $j -eq 21 ]; then
			continue
		fi
		echo "## Running tpch query #"$j", partitions=$partitions, decoders=$decoders, encoders=$encoders" >>$log
		./tpch-spark-q.sh $j $partitions $decoders $encoders >>$log 2>&1
		echo >>$log
		./stable-test-avg-result.sh $log > $log.md
	done
done
