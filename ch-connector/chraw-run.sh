query="$1"

set -eu

source _env.sh

if [ -z "$query" ]; then
	echo "usage: <bin> query-string" >&2
	exit 1
fi

build/dbms/src/Magic/ch-raw "running/config/config.xml" "$query"
