n="$1"
partitionsPerSplit="$2"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitionsPerSplit]" >&2
	exit 1
fi

source ./_env.sh

if [ -z "$partitionsPerSplit" ]; then
	partitionsPerSplit="$default_partitionsPerSplit"
fi

file="./tpch/sql/q"$n".sql"
sql=`cat $file | tr '\n' ' '`

echo "## Running tpch query #"$n", partitionsPerSplit=$partitionsPerSplit"
./spark-q.sh "$sql" "$partitionsPerSplit"
echo
