#!/bin/bash

server="$1"
query="$2"
format="$3"

set -eu

if [ -z "$server" ]; then
	echo "usage: <bin> server-address(host, or host:port) [query-sql] [output-format]">&2
	exit 1
fi

if [ ! -z "$format" ]; then
	format="-f $format"
fi

source ./_env.sh

"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" \
	--query="create database if not exists $storage_db"

if [ -z "$query" ]; then
	"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" -d "$storage_db"
else
	"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" \
		-d "$storage_db" --query="$query" $format
fi
