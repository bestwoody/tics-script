table="$1"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name" >&2
	exit 1
fi

source _env.sh

"$storage_bin" client --host="$storage_server" -d "$storage_db" --query="drop table $table"
