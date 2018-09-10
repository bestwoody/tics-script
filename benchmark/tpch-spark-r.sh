#!/bin/bash

n="$1"
log="$2"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [log-file=./tpch.log]" >&2
	exit 1
fi

if [ -z $log ]; then
	log="./tpch.log"
fi

./tpch-spark-q.sh "$n" >> "$log"
./tpch-gen-report.sh "$log" > "$log.md"
