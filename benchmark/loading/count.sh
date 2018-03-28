table="$1"
selraw="$2"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name [selraw=false]" >&2
	exit 1
fi

select="select"
if [ "$selraw" == "true" ]; then
	select="selraw"
fi

source _env.sh

echo [$table]
"$chbin" client --host="$chserver" -d "$chdb" --query="$select count() from $table"
