source ./_helper.sh

# Client executable path
export storage_bin="$repo_dir/storage/build/dbms/src/Server/theflash"
# Storage server list, eg: ("127.0.0.1"), or: ("127.0.0.1:9000" "127.0.0.1:9006")
export storage_server=("127.0.0.1")
# Database to receive data
export storage_db="default"

# Dbgen executable path
export dbgen_dir="$repo_dir/benchmark/tpch-dbgen"
# Meta data path, 'mmt-meta' is for MutableMergeTree, and 'meta' is for the other engines
export meta_dir="$this_dir/mmt-meta"
export meta_dir="$this_dir/meta"

# Prefix of dir name for dbgen output files.
export db_prefix="tpch"
# TPCH scale, '2' means TPCH-2 (2G), '100' means 100G
export tpch_scale="1"
# Parallel loading threads, should < cpu.cores
export tpch_blocks="4"

# Internal vars, don't modify these
export database="$db_prefix$tpch_scale"
export dbgen_result_dir_prefix="$this_dir"/"$db_prefix"-"$tpch_scale"-c

# Setup running env vars
source ./_vars.sh
setup_dylib_path
