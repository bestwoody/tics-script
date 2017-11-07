set -eu

dir="running/clickhouse"
mkdir -p "$dir/data"
build/dbms/src/Server/clickhouse client
