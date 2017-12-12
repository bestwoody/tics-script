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

schema="$meta_dir/schema/$table.schema"
if [ ! -f "$schema" ]; then
	echo "$schema schema file not found" >&2
	exit 1
fi
"$chbin" client --host="$chserver" --query="`cat $schema`"

set -e
import_table "$blocks" "$table"
