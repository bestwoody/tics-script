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
	./stable-test.sh 1 $partitions $decoders $encoders $log
	./stable-test-parquet.sh
done

for (( i = 0; i < $count; i++ )); do
	./unstable-test.sh 1 $partitions $decoders $encoders $log
done
