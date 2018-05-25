# Executable path
export storage_bin="storage/theflash"

# Storage server address for scripts and Spark
export storage_server="127.0.0.1"

# Storage server config file path
export storage_server_config="storage/config.xml"

# Default database when we run scripts or in Spark
export storage_db="default"

# RDD partition number of any query launch by CHSpark
export default_partitions="16"

# Arrow decoding thread number of each RDD partition
export default_decoders="1"

# Total Arrow encoding thread number of a query launch by CHSpark
export default_encoders="16"

# Use SELRAW for any query launch by CHSpark (except fetch table info)
# DON'T set to 'true' for Non-Mutable table
# Should be always true on Mutable table, unless for tracing MutableMergeTree bugs
export selraw="true"

# Use SELRAW to fetch table info, can faster than SELECT on MutableMergeTree. DON'T set to 'true' for Non-Mutable table
export selraw_tableinfo="false"

# Spark master to commit jobs
export spark_master="127.0.0.1"

# Pushdown aggregation ops, should be always true unless for tracing debugs
export pushdown="true"

# Aggressive optimization for single-node table, i.e. pushdown more aggregates.
# Should be always true unless for debug purpose.
export single_node_optimization="true"

# The executor-memory parameter of spark-shell.
export spark_executor_memory=12G

# Setup running env vars
export LD_LIBRARY_PATH="`dirname $storage_bin`:/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib"
