source _helper.sh

export DYLD_LIBRARY_PATH=""
export LD_LIBRARY_PATH="/usr/local/lib64:$LD_LIBRARY_PATH"

export chbin="$repo_dir/ch-connector/build/dbms/src/Server/clickhouse"
export chserver="127.0.0.1"
