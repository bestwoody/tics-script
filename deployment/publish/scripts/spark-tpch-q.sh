n="$1"
partitions="$2"
decoders="$3"
encoders="$4"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitions] [decoders] [encoders]" >&2
	exit 1
fi

source ./_env.sh

if [ -z "$partitions" ]; then
	partitions="$default_partitions"
fi
if [ -z "$decoders" ]; then
	decoders="$default_decoders"
fi
if [ -z "$encoders" ]; then
	encoders="$default_encoders"
fi

file="./tpch/sql/q"$n".sql"
sql=`cat $file | tr '\n' ' '`

echo "## Running tpch query #"$n", partitions=$partitions, decoders=$decoders, encoders=$encoders"
./spark-q.sh "$sql" "$partitions" "$decoders" "$encoders"
echo
