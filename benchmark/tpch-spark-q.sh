n="$1"
print="$2"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [print]"
	exit 1
fi

file="./sql-spark/q"$n".sql"
sql=`cat $file | tr '\n' ' '`
tmp="$file.running"

if [ "$print" == "print" ]; then
	echo "$sql"
else
	./spark-q.sh "$sql" "$tmp"
fi
