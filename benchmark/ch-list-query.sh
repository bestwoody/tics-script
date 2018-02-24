set -eu
query="SELECT query, query_id, elapsed, read_rows, memory_usage, client_name FROM system.processes"
source _env.sh
"$chbin" client --host "$chserver" --query "$query" -f Vertical
