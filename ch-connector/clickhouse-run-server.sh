set -eu

dir="running/clickhouse"
mkdir -p "$dir/data"
build/dbms/src/Server/clickhouse >> "$dir/server.log" 2>&1
