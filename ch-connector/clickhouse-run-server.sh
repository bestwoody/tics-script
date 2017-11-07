set -eu

dir="running/clickhouse"
mkdir -p "$dir/data"
build/dbms/src/Server/clickhouse server --config-file "running/config/config.xml" > "$dir/server-main.log" 2>&1
