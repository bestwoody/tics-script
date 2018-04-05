table="$1"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name" >&2
	exit 1
fi

source _env.sh

"$chbin" client --host="$chserver" -d "$chdb" --query="drop table $table"