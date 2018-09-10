#!/bin/bash

set -eu

source ./_env.sh

if [ "$#" -lt 1 ]; then
	echo "<bin> usage: <bin> q1 q2 ... " >&2
	exit 1
fi

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

print_spark_settings > "$tmp"

for sql in "$@"; do
	if [ ! -z "$sql" ]; then
		echo "spark.sql(\"$sql\")" >> "$tmp"
	fi
done

./spark-shell.sh < "$tmp"

rm -f "$tmp"
