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
		if [ $j -ne 17 ] && [ $j -ne 18 ] && [ $j -ne 20 ] && [ $j -ne 21 ]; then
			continue
		fi
		./tpch-spark-r.sh "$j" "$log"
	done
done
