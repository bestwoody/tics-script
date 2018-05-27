n="$1"
partitions="$2"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitions]" >&2
	exit 1
fi

source ./_env.sh

if [ -z "$partitions" ]; then
	partitions="$default_partitions"
fi

file="./tpch-sql/q"$n".sql"
sql=`cat $file | tr '\n' ' '`

echo "## Running tpch query #"$n", partitions=$partitions"
./spark-q.sh "$sql" "$partitions"
echo
