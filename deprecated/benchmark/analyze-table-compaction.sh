#!/bin/bash

table="$1"
db_name="$2"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-to-be-analyze [database-name(default: from _env.sh)]" >&2
	exit 1
fi

source ./_env.sh

if [ -z "$db_name" ]; then
	db_name="$storage_db"
fi

data_path=`grep '<path>' "$storage_server_config" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
table_path="$data_path/data/$db_name/$table"
if [ ! -d "$table_path" ]; then
	echo "storage path: $storage_server_config (from _env.sh)" >&2
	echo "database: $db_name (from _env.sh or script args)" >&2
	echo "table: $table (from args)" >&2
	echo "error: table path \"$data_path/data/$db_name/$table\" not found, exited" >&2
	exit 1
fi

curr_path=`pwd`
cd "$data_path/data/$db_name/$table"

du -sk * | python "$curr_path/analyze-table-compaction.py"

cd "$curr_path"
