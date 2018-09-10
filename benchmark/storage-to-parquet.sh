#!/bin/bash

set -eu

source ./_env.sh

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'import java.util.Date' >> "$tmp"
echo 'import spark.implicits._' >> "$tmp"

print_spark_settings >> "$tmp"

echo 'val startTime = new Date()' >> "$tmp"

echo "spark.sql(\"use $storage_db\")" >> "$tmp"

echo "spark.sql(\"select * from lineitem \").write.parquet(\"parquet/lineitem3\")" >> "$tmp"
echo "spark.sql(\"select * from customer \").write.parquet(\"parquet/customer3\")" >> "$tmp"
echo "spark.sql(\"select * from nation \").write.parquet(\"parquet/nation3\")" >> "$tmp"
echo "spark.sql(\"select * from part \").write.parquet(\"parquet/part3\")" >> "$tmp"
echo "spark.sql(\"select * from region \").write.parquet(\"parquet/region3\")" >> "$tmp"
echo "spark.sql(\"select * from orders \").write.parquet(\"parquet/orders3\")" >> "$tmp"
echo "spark.sql(\"select * from partsupp \").write.parquet(\"parquet/partsupp3\")" >> "$tmp"
echo "spark.sql(\"select * from supplier \").write.parquet(\"parquet/supplier3\")" >> "$tmp"

echo 'val elapsed = (new Date().getTime - startTime.getTime) / 100 / 10.0' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
