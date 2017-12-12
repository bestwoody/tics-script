set -eu

source ./_env.sh

build/dbms/src/Server/clickhouse client --query "$@"
