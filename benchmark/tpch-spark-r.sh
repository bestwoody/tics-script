#!/bin/bash

n="$1"
partitionsPerSplit="$2"
log="$3"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> n(1|2|3|...) [partitionsPerSplit] [log-file=./tpch.log]" >&2
	exit 1
fi

if [ -z $log ]; then
	log="./tpch.log"
fi

./tpch-spark-q.sh "$n" "$partitionsPerSplit" >> "$log"
./tpch-gen-report.sh "$log" > "$log.md"
