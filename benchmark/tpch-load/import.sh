table="$1"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name" >&2
	exit 1
fi

source _import.sh

"$storage_bin" client --host="$storage_server" --query="create database if not exists $storage_db"
if [ $? != 0 ]; then
	echo "create database '"$storage_db"' failed" >&2
	exit 1
fi

schema="$meta_dir/schema/$table.schema"
if [ ! -f "$schema" ]; then
	echo "$schema schema file not found" >&2
	exit 1
fi

"$storage_bin" client --host="$storage_server" -d "$storage_db" --query="`cat $schema`"

import_table "$tpch_blocks" "$table"
