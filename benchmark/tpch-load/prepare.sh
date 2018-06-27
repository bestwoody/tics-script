table="$1"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name" >&2
	exit 1
fi

source ./_dbgen.sh
source ./_trans.sh

dbgen "$tpch_blocks" "$table"
trans_table "$tpch_blocks" "$table"
