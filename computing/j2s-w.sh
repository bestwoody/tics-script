#!/bin/bash

database="$1"
table="$2"
client_batch="$3"
storage_batch="$4"
rows="$5"
threads="$6"
query="$7"
same_value="$8"
verb="$9"
host="${10}"
port="${11}"

set -eu

if [ -z "$rows" ]; then
	echo "usage: <bin> database table sending-batch-rows-count writing-batch-rows-count insert-rows-count threads [pre-create-table-sql=''] [use-same-value=false] [verb=2] [host] [port]" >&2
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
	"$database" "$table" "$client_batch" "$storage_batch" "$rows" "$threads" "$query" "$same_value" "$verb" "$host" "$port" \
	2>&1 | grep -v 'SLF4J' | grep -v 'log4j'
