source _helper.sh
source _vars.sh

# ClickHouse executable path
export chbin="$repo_dir/ch-connector/build/dbms/src/Server/clickhouse"
# ClickHouse server address to receive data
export chserver="127.0.0.1"
# ClickHouse database to receive data
export chdb="mutable"
export chdb="default"

# Dbgen executable path
export dbgen_dir="$repo_dir/benchmark/tpch-dbgen"
# Meta data path, 'mmt-meta' is for MutableMergeTree, and 'meta' is for the other engines
export meta_dir="$this_dir/mmt-meta"
export meta_dir="$this_dir/meta"

# Prefix of dir name for dbgen output files.
export db_prefix="tpch"
# TPCH scale, '2' means TPCH-2 (2G), '100' means 100G
export tpch_scale="2"
# Parallel loading threads, should < cpu.cores
export tpch_blocks="1"

# Internal vars, don't modify these
export database="$db_prefix$tpch_scale"
export dbgen_result_dir_prefix="$this_dir"/"$db_prefix"-"$tpch_scale"-c
