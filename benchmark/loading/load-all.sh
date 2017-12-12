blocks="$1"

set -eu

if [ -z "$blocks" ]; then
	echo "usage: <bin> [block-numbers]" >&2
	exit 1
fi

source _meta.sh

get_table_names | while read table; do
	./load-one.sh "$table" "$blocks"
done
