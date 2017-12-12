table="$1"
blocks="$2"

set -eu

if [ -z "$table" ] && [ -z "$blocks" ]; then
	echo "usage: <bin> [table-name] [block-numbers]" >&2
	exit 1
fi

source _dbgen.sh

if [ -z "$blocks" ]; then
	blocks="$tpch_blocks"
fi

dbgen "$blocks" "$table"
