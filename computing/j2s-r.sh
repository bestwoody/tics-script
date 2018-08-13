#!/bin/bash

query="$1"
verb="$2"
host="$3"
port="$4"

set -eu

if [ -z "$query" ]; then
	echo "usage: <bin> query [verb=0|1|2] [host] [port]" >&2
	exit 1
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
	org.apache.spark.sql.ch/CHRawReader \
	"$query" "$verb" "$host" "$port" \
	2>&1 | grep -v 'SLF4J' | grep -v 'log4j'
