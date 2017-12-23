n="$1"
partitions="$2"
decoders="$3"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitions=8] [decoders=8]" >&2
	exit 1
fi

file="./sql-spark/q"$n".sql"
sql=`cat $file | tr '\n' ' '`
tmp="$file.running"

./spark-q.sh "$sql" "$tmp" "$partitions" "$decoders"
