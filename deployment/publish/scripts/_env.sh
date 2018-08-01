#!/bin/bash

source ./_helper.sh

# Executable path
export storage_bin="storage/theflash"

# Storage server list for scripts and Spark, eg: ("127.0.0.1"), or: ("127.0.0.1:9000" "127.0.0.1:9006")
export storage_server=("127.0.0.1")

# Storage server config file path
export storage_server_config="storage/config.xml"

# Default database when we run scripts or in Spark
export storage_db="default"

# The number of partitions in CH that one RDD of Spark handle.
export default_partitionsPerSplit="2"

# Use SELRAW for any query launch by CHSpark (except fetch table info)
# DON'T set to 'true' for Non-Mutable table
# Should be always true on Mutable table, unless for tracing MutableMergeTree bugs
export selraw="false"

# Spark master to commit jobs
export spark_master="127.0.0.1"

# Pushdown aggregation ops, should be always true unless for tracing debugs
export pushdown="true"

# The executor-memory parameter of spark-shell.
export spark_executor_memory=12G

# Setup running env vars
source ./_vars.sh
setup_dylib_path
