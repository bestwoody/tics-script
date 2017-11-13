set -eu

mkdir -p "running/clickhouse/db"
DYLD_LIBRARY_PATH="" build/dbms/src/Server/clickhouse server --config-file "running/config/config.xml"
