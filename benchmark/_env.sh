source _helper.sh

# *.dyso/*.a path for mac os
export DYLD_LIBRARY_PATH=""
# *.so/*.a path for linux
export LD_LIBRARY_PATH="/usr/lib:/usr/local/lib:/usr/lib64:/usr/local/lib64"

# Executable path
export chbin="$repo_dir/ch-connector/build/dbms/src/Server/theflash"
# Server address for scripts and Spark
export chserver="127.0.0.1"
# Default database when we run scripts or in Spark
export chdb="mutable"
export chdb="default"

# Spark master to commit jobs
export spark_master="127.0.0.1"

# RDD partition number of any query launch by CHSpark
export default_partitions="16"
# Arrow decoding thread number of each RDD partition
export default_decoders="1"
# Total Arrow encoding thread number of a query launch by CHSpark
export default_encoders="16"

# Pushdown aggregation ops, should be always true unless for tracing debugs
export pushdown="true"
# Use code gen, should be always true unless for tracing bugs
export codegen="true"

# Use SELRAW for any query launch by CHSpark (except fetch table info)
# Should be always true unless for tracing MutableMergeTree bugs
export selraw="false"

# Use SELRAW to fetch table info, can faster than SELECT on MutableMergeTree. DON'T set to 'true' for non-Mutable table
export selraw_tableinfo="false"
