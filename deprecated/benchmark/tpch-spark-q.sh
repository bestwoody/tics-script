#!/bin/bash

n="$1"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...)" >&2
	exit 1
fi

source ./_env.sh

file="./tpch-sql/q"$n".sql"
sql=`cat $file | tr '\n' ' '`

echo "## Running tpch query #"$n", partitionsPerSplit=$partitionsPerSplit"
./spark-q.sh "$sql"
echo
