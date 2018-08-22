#!/bin/bash

database="$1"
table="$2"
threads="$3"
query="$4"
verb="$5"
host="$6"
port="$7"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> database table [threads=8] [query=\"select \* from database.table\"] [verb=1] [host] [port]" >&2
	exit 1
fi

if [ -z "$threads" ]; then
	threads="8"
fi
if [ -z "$verb" ]; then
	verb="1"
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
	"$database" "$table" "$threads" "$query" "$verb" "$host" "$port" \
	2>&1 | grep -v 'SLF4J' --line-buffered | grep -v 'log4j' --line-buffered
