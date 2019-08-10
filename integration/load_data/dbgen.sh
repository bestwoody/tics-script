#!/bin/bash

table="$1"
blocks="$2"

set -eu
set -o pipefail

source _env_load.sh

if [ -z "$blocks" ]; then
	echo "usage: <bin> table-name blocks-number" >&2
	exit 1
fi

if [ "$table" == "lineitem" ]; then
	table="L"
fi
if [ "$table" == "partsupp" ]; then
	table="S"
fi
if [ "$table" == "customer" ]; then
	table="c"
fi
if [ "$table" == "nation" ]; then
	table="n"
fi
if [ "$table" == "orders" ]; then
	table="O"
fi
if [ "$table" == "part" ]; then
	table="P"
fi
if [ "$table" == "region" ]; then
	table="r"
fi
if [ "$table" == "supplier" ]; then
	table="s"
fi


if [ -z "$table" ]; then
	echo "unkown table name, exit ..." >&2
	exit 1
fi

cd "./bin"
for ((i=1; i<$blocks+1; ++i)); do
	.//dbgen -C "$blocks" -T "$table" -s ${scale} -S "$i" -f &
	#.//dbgen -C "$blocks" -T "$table" -s ${scale} -f &
done

wait
