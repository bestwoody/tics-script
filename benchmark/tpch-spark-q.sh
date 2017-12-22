n="$1"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...)"
	exit 1
fi

file="./sql-spark/q"$n".sql"
sql=`cat $file | tr '\n' ' '`
tmp="$file.running"

./spark-q.sh "$sql" "$tmp"
