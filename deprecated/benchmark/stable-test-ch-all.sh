#!/bin/bash

count="$1"
log="$2"

set -eu

if [ -z "$count" ]; then
	count=5
fi

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 22; j++ )); do
		if [ $j -eq 15 ]; then
			continue
		fi
		./tpch-spark-r.sh "$j" "$log"
	done
done
