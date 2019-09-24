#!/bin/bash

sql="$1"
query_db="$2"
set -eu

source ./_env.sh

if [ -z "$query_db" ]; then
	query_db="$storage_db"
fi

if [ -z "$sql" ]; then
	echo "<bin> usage: <bin> query-sql [query_db]" >&2
	exit 1
fi

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'import java.text.SimpleDateFormat' > "$tmp"
echo 'import java.util.Date' >> "$tmp"

print_spark_settings >> "$tmp"

echo 'val startTime = new Date()' >> "$tmp"

echo "spark.sql(\"use $query_db\")" >> "$tmp"

echo "spark.sql(\"$sql\").show(false)" >> "$tmp"

echo 'val elapsed = (new Date().getTime - startTime.getTime) / 100 / 10.0' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
