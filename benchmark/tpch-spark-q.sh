n="$1"
partitions="$2"
decoders="$3"
encoders="$4"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitions=4] [decoders=2] [encoders=4]" >&2
	exit 1
fi

file="./sql-spark/q"$n".sql"
sql=`cat $file | tr '\n' ' '`

./spark-q.sh "$sql" "$partitions" "$decoders" "$encoders"
