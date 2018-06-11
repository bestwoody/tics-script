table="$1"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name" >&2
	exit 1
fi

source _dbgen.sh
source _trans.sh
source _import.sh

blocks="$tpch_blocks"

dbgen "$blocks" "$table"
trans_table "$blocks" "$table"
import_table "$blocks" "$table"
