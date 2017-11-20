blocks="$1"
table="$2"

set -eu

if [ -z "$blocks" ] && [ -z "$blocks" ]; then
	echo "usage: <bin> [block-numbers] [table-name]"
	exit 1
fi

source _dbgen.sh

if [ -z "$blocks" ]; then
	blocks="$tpch_blocks"
fi

dbgen "$blocks" "$table"
