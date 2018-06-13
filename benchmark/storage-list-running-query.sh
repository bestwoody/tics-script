set -eu

query="SELECT query, query_id, elapsed, read_rows, memory_usage, client_name FROM system.processes"

source ./_env.sh

if [ ${#storage_server[@]} -gt 1 ]; then
	for server in ${storage_server[@]}; do
		echo [$server]
		"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" --query "$query" -f Vertical
		echo
	done
else
	server=${storage_server[0]}
	"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" --query "$query" -f Vertical
fi
