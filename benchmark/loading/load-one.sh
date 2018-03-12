table="$1"
blocks="$2"

set -u

if [ -z "$table" ] && [ -z "$blocks" ]; then
	echo "usage: <bin> [table-name] [block-numbers]" >&2
	exit 1
fi

source _dbgen.sh
source _trans.sh
source _import.sh

if [ -z "$blocks" ]; then
	blocks="$tpch_blocks"
fi

dbgen "$blocks" "$table"
trans_table "$blocks" "$table"

"$chbin" client --host="$chserver" -d "$chdb" --query="create database if not exists $chdb"
if [ $? != 0 ]; then
	echo "create database '"$chdb"' failed" >&2
	exit 1
fi

schema="$meta_dir/schema/$table.schema"
if [ ! -f "$schema" ]; then
	echo "$schema schema file not found" >&2
	exit 1
fi
"$chbin" client --host="$chserver" -d "$chdb" --query="`cat $schema`"

set -e
import_table "$blocks" "$table"
