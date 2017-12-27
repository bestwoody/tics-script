set -eu
source _env.sh
build/dbms/src/Server/clickhouse client --host 127.0.0.1 --query "$@"
