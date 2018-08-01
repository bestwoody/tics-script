#!/bin/bash

count="$1"
partitionsPerSplit="$2"
log="$3"

set -eu

if [ -z "$count" ]; then
	count=5
fi

for (( i = 0; i < $count; i++ )); do
	for (( j = 1; j <= 22; j++ )); do
		if [ $j -eq 15 ]; then
			continue
		fi
		if [ $j -eq 17 ] || [ $j -eq 18 ] || [ $j -eq 20 ] || [ $j -eq 21 ]; then
			continue
		fi
		./tpch-spark-r.sh "$j" "$partitionsPerSplit" "$log"
	done
done
