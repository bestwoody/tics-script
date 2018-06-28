table="$1"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-to-be-analyze" >&2
	exit 1
fi

source ./_env.sh

data_path=`grep '<path>' "$storage_server_config" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`

curr_path=`pwd`
cd "$data_path/data/$storage_db/$table"

du -sk * | python "$curr_path/analyze-table-compaction.py"

cd "$curr_path"
