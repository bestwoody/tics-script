#!/bin/bash

database="$1"
table="$2"
client_batch="$3"
storage_batch="$4"
rows="$5"
threads="$6"
host="$7"
port="$8"

storage_batch_bytes="100000000000"
same_value="false"
verb="2"

set -eu

if [ -z "$database" ]; then
	echo "usage: <bin> database [table-name=j2s_medium(as: j2s-cases/{table-name}.sql)] [sending-batch-rows-count=524288] [writing-batch-rows-count=10485760] [insert-rows-count=20480000] [threads=4] [host] [port]" >&2
	exit 1
fi

if [ -z "$table" ]; then
	table="j2s_medium"
fi
if [ -z "$client_batch" ]; then
	client_batch="524288"
fi
if [ -z "$storage_batch" ]; then
	storage_batch="10485760"
fi
if [ -z "$rows" ]; then
	rows="20480000"
fi
if [ -z "$threads" ]; then
	threads="8"
fi
if [ -z "$host" ]; then
	host="127.0.0.1"
fi
if [ -z "$port" ]; then
	port="9000"
fi

query=`cat j2s-cases/${table}.sql | tr '\n' ' '`

./j2s-w.sh "$database" "$table" "$client_batch" "$storage_batch" "$storage_batch_bytes" "$rows" "$threads" "$query" "$same_value" "$verb" "$host" "$port"
