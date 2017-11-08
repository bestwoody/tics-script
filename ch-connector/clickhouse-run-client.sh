set -eu
build/dbms/src/Server/clickhouse client --query "$@"
