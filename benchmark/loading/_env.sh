source _helper.sh
source _vars.sh

export chbin="$repo_dir/ch-connector/build/dbms/src/Server/clickhouse"
export chserver="127.0.0.1"

export dbgen_dir="$repo_dir/benchmark/tpch-dbgen"
export meta_dir="$this_dir/meta"

export db_prefix="tpch"
export tpch_scale="100"
export tpch_blocks="4"

export database="$db_prefix$tpch_scale"
export dbgen_result_dir_prefix="$this_dir"/"$db_prefix"-"$tpch_scale"-c
