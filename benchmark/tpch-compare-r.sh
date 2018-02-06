n="$1"
partitions="$2"
decoders="$3"
encoders="$4"
log="$5"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitions] [decoders] [encoders] [log-file=./tpch.log]" >&2
	exit 1
fi

./tpch-spark-r.sh "$n" "$partitions" "$decoders" "$encoders" "$log"
./tpch-parquet-r.sh "$n" "$log"
