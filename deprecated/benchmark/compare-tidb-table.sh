#!/bin/bash

tidb_db="$1"
tidb_tab="$2"
set -eu

source ./_env.sh

if [ -z "$tidb_db" ] || [ -z "$tidb_tab" ]; then
	echo "<bin> usage: <bin> tidb_db tidb_tab" >&2
	exit 1
fi

mkdir -p "/tmp/compare-tidb-table/"
tmp="/tmp/compare-tidb-table/`date +%s`"

print_spark_settings >> "$tmp"

echo "val ti = new org.apache.spark.sql.TiContext(spark)" >> "$tmp"

echo "val tiDf = ti.getDataFrame(\"$tidb_db\", \"$tidb_tab\")" >> "$tmp"
echo "val chDf = spark.sql(\"select * from $tidb_db.$tidb_tab\")" >> "$tmp"

echo "val tiCount = tiDf.count" >> "$tmp"
echo "val chCount = chDf.count" >> "$tmp"
echo "if (tiCount != chCount) {" >> "$tmp"
echo "  println(\"Count not match!!!\")" >> "$tmp"
echo "  println(\"TiDB count:\" + tiCount)" >> "$tmp"
echo "  println(\"CH count:\" + chCount)" >> "$tmp"
echo "  System.exit(1)" >> "$tmp"
echo "}" >> "$tmp"

echo "val diff = tiDf.except(chDf).count" >> "$tmp"
echo "if (diff != 0) {" >> "$tmp"
echo "  println(\"Rows not match!!!\")" >> "$tmp"
echo "  println(\"Diff count:\" + diff)" >> "$tmp"
echo "  System.exit(1)" >> "$tmp"
echo "}" >> "$tmp"

echo "println(\"MATCH!!!\")" >> "$tmp"

./spark-shell.sh --conf "spark.tispark.show_rowid=false" < "$tmp"

rm -f "$tmp"
