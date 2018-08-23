#!/bin/bash

database="$1"
table="$2"
client_batch="$3"
storage_batch_rows="$4"
storage_batch_bytes="$5"
rows="$6"
threads="$7"
query="$8"
same_value="$9"
verb="${10}"
host="${11}"
port="${12}"

set -eu

if [ -z "$rows" ]; then
	echo "usage: <bin> database table sending-batch-rows-count writing-batch-rows writing-batch-bytes insert-rows-count threads [pre-create-table-sql=''] [use-same-value=false] [verb=2] [host] [port]" >&2
	exit 1
fi

if [ -z "$same_value" ]; then
	same_value="false"
fi
if [ -z "$verb" ]; then
	verb="2"
fi
if [ -z "$host" ]; then
	host="127.0.0.1"
fi
if [ -z "$port" ]; then
	port="9000"
fi

java -XX:MaxDirectMemorySize=5g \
	-cp chspark/target/*:chspark/target/lib/*:spark/jars/* \
	org.apache.spark.sql.ch/CHRawWriter \
	"$database" "$table" "$client_batch" "$storage_batch_rows" "$storage_batch_bytes" "$rows" "$threads" "$query" "$same_value" "$verb" "$host" "$port" \
	2>&1 | grep -v 'SLF4J' --line-buffered | grep -v 'log4j' --line-buffered
