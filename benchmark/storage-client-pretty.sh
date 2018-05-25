arg="$1"

set -eu
source ./_env.sh

"$storage_bin" client --host="$storage_server" --query="create database if not exists $storage_db"
if [ $? != 0 ]; then
	echo "create database '"$storage_db"' failed" >&2
	exit 1
fi

if [ -z "$arg" ]; then
	"$storage_bin" client --host "$storage_server" -d "$storage_db"
else
	"$storage_bin" client --host "$storage_server" -d "$storage_db" --query "$@" -f PrettyCompactNoEscapes
fi
