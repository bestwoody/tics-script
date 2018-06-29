table="$1"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name" >&2
	exit 1
fi

source ./_env.sh

for server in ${storage_server[@]}; do
	"$storage_bin" client --host=`get_host $server` --port=`get_port $server` \
		-d "$storage_db" --query="drop table if exists $table"
done
