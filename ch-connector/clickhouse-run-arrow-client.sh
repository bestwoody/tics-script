set -eu
DYLD_LIBRARY_PATH="" build/dbms/src/Server/clickhouse client --port 9006 --query "$@"
