set -eu

mkdir -p "running/clickhouse/db"
build/dbms/src/Server/clickhouse server --config-file \
	"running/config/config.xml" > "running/clickhouse/server-main.log" 2>&1
