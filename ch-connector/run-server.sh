set -eu
source _env.sh
mkdir -p "running/clickhouse/db"
"$chbin" server --config-file "running/config/config.xml"
