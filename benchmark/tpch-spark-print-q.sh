n="$1"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...)" >&2
	exit 1
fi

file="./tpch-sql/q"$n".sql"
cat $file | tr '\n' ' ' | tr '\t' ' ' | tr '  ' ' '
echo
