query="$1"
format="$2"

set -eu

if [ ! -z "$format" ]; then
	format="-f $format"
fi

source ./_env.sh

for server in ${storage_server[@]}; do
	"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" \
		--query="create database if not exists $storage_db"
done

if [ -z "$query" ]; then
	if [ ${#storage_server[@]} -gt 1 ]; then
		echo "WARNNING: more than one server defined in _env.sh, connecting the first one..." >&2
		echo
	fi
	server=${storage_server[0]}
	"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" -d "$storage_db"
else
	if [ ${#storage_server[@]} -gt 1 ]; then
		for server in ${storage_server[@]}; do
			echo [$server]
			"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" \
				-d "$storage_db" $format --query="$query"
			echo
		done
	else
		server=${storage_server[0]}
		"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" \
			-d "$storage_db" --query="$query" $format
	fi
fi
