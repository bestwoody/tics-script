source _helper.sh

export DYLD_LIBRARY_PATH=""
export LD_LIBRARY_PATH="/usr/lib:/usr/local/lib:/usr/lib64:/usr/local/lib64"

export chbin="$repo_dir/ch-connector/build/dbms/src/Server/clickhouse"
export chserver="127.0.0.1"

export default_partitions="16"
export default_decoders="4"
export default_encoders="16"

export pushdown="true"
export codegen="true"
