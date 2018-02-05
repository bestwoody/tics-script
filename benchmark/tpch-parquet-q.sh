n="$1"
tmp="$2"

set -eu

if [ -z "$n" ]; then
	echo "<bin> usage: <bin> tpch-query-number(like: 6) [tmp-sql-file]" >&2
	exit 1
fi

if [ -z "$tmp" ]; then
	mkdir -p "/tmp/spark-q/"
	tmp="/tmp/spark-q/`date +%s`"
fi

echo 'import java.util.Date' >> "$tmp"
echo 'import spark.implicits._' >> "$tmp"

cat /data/table/q"$n" | while read table; do
       echo "val ${table}3 = spark.read.parquet(\"parquet/${table}3\")" >> "$tmp"
	   echo "${table}3.createOrReplaceTempView(\"${table}\")" >> "$tmp"
done

echo 'val startTime = new Date()' >> "$tmp"

sql=`cat sql-spark/q"$n".sql | tr '\n' ' '`
echo "spark.sql(\"$sql\").show" >> "$tmp"

echo 'val elapsed = (new Date().getTime - startTime.getTime) / 100 / 10.0' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
