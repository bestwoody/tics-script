set -eu

source ./_env.sh

partitionsPerSplit="4"

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'import java.util.Date' >> "$tmp"
echo 'import spark.implicits._' >> "$tmp"

spart_settings "$tmp"

echo 'val storage = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"

server=${storage_server[0]}
"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" -d "$storage_db" --query="show tables" | \
	while read table; do
	echo "storage.mapCHClusterTable(database=\"$storage_db\", table=\"$table\", partitionsPerSplit=$partitionsPerSplit)" >> "$tmp"
done

echo 'val startTime = new Date()' >> "$tmp"
echo "storage.sql(\"select * from lineitem \").write.parquet(\"parquet/lineitem3\")" >> "$tmp"

echo "storage.sql(\"select * from customer \").write.parquet(\"parquet/customer3\")" >> "$tmp"
echo "storage.sql(\"select * from nation \").write.parquet(\"parquet/nation3\")" >> "$tmp"
echo "storage.sql(\"select * from part \").write.parquet(\"parquet/part3\")" >> "$tmp"
echo "storage.sql(\"select * from region \").write.parquet(\"parquet/region3\")" >> "$tmp"
echo "storage.sql(\"select * from orders \").write.parquet(\"parquet/orders3\")" >> "$tmp"
echo "storage.sql(\"select * from partsupp \").write.parquet(\"parquet/partsupp3\")" >> "$tmp"
echo "storage.sql(\"select * from supplier \").write.parquet(\"parquet/supplier3\")" >> "$tmp"

echo 'val elapsed = (new Date().getTime - startTime.getTime) / 100 / 10.0' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
