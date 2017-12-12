query="$1"

set -eu

source _env.sh

if [ -z "$query" ]; then
	query="SELECT * FROM test"
fi

build/dbms/src/Magic/ch-raw "running/config/config.xml" "$query"
