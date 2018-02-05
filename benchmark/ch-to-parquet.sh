n="$1"
partitions="$2"
decoders="$3"
tmp="$4"

set -eu

if [ -z "$partitions" ]; then
	partitions="8"
fi
if [ -z "$decoders" ]; then
	decoders="8"
fi

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> query-sql [partitions=8] [decoders=8] [tmp-sql-file]" >&2
	exit 1
fi

if [ -z "$tmp" ]; then
	mkdir -p "/tmp/spark-q/"
	tmp="/tmp/spark-q/`date +%s`"
fi

echo 'import java.util.Date' >> "$tmp"
echo 'import spark.implicits._' >> "$tmp"
echo 'val ch = new org.apache.spark.sql.CHContext(spark,true)' >> "$tmp"

./ch-q.sh "show tables" | while read table; do
	echo "ch.mapCHClusterTable(table=\"$table\", partitions=$partitions, decoders=$decoders)" >> "$tmp"
done

echo 'val startTime = new Date()' >> "$tmp"
echo "ch.sql(\"select * from lineitem \").write.parquet(\"parquet/lineitem3\")" >> "$tmp"

echo "ch.sql(\"select * from customer \").write.parquet(\"parquet/customer3\")" >> "$tmp"
echo "ch.sql(\"select * from nation \").write.parquet(\"parquet/nation3\")" >> "$tmp"
echo "ch.sql(\"select * from part \").write.parquet(\"parquet/part3\")" >> "$tmp"
echo "ch.sql(\"select * from region \").write.parquet(\"parquet/region3\")" >> "$tmp"
echo "ch.sql(\"select * from orders \").write.parquet(\"parquet/orders3\")" >> "$tmp"
echo "ch.sql(\"select * from partsupp \").write.parquet(\"parquet/partsupp3\")" >> "$tmp"
echo "ch.sql(\"select * from supplier \").write.parquet(\"parquet/supplier3\")" >> "$tmp"

echo 'val elapsed = (new Date().getTime - startTime.getTime) / 100 / 10.0' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"