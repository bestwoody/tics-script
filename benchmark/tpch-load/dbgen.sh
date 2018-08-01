#!/bin/bash

table="$1"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name" >&2
	exit 1
fi

source ./_dbgen.sh

dbgen "$tpch_blocks" "$table"
