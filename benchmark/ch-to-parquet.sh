set -eu

source ./_env.sh

partitions="8"
decoders="8"

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'import java.util.Date' >> "$tmp"
echo 'import spark.implicits._' >> "$tmp"

echo 'spark.conf.set("spark.ch.plan.codegen", "true")' >> "$tmp"
echo 'spark.conf.set("spark.ch.plan.pushdown.agg", "true")' >> "$tmp"

echo 'val ch = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"

./ch-q.sh "show tables" | while read table; do
	echo "ch.mapCHClusterTable(database=\"$chdb\", table=\"$table\", partitions=$partitions, decoders=$decoders)" >> "$tmp"
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
