sql="$1"
partitionsPerSplit="$2"
query_db="$3"
set -eu

source ./_env.sh

if [ -z "$partitionsPerSplit" ]; then
	partitionsPerSplit="$default_partitionsPerSplit"
fi

if [ -z "$query_db" ]; then
	query_db="$storage_db"
fi

if [ -z "$sql" ]; then
	echo "<bin> usage: <bin> query-sql [partitionsPerSplit]" >&2
	exit 1
fi

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'import java.text.SimpleDateFormat' > "$tmp"
echo 'import java.util.Date' >> "$tmp"

print_spark_settings >> "$tmp"

echo 'val storage = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"

server=${storage_server[0]}
"$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" -d "$query_db" --query="show tables" | \
	while read table; do
	echo "storage.mapCHClusterTable(database=\"$query_db\", table=\"$table\", partitionsPerSplit=$partitionsPerSplit)" >> "$tmp"
done

echo 'val startTime = new Date()' >> "$tmp"

echo "storage.sql(\"$sql\").show(false)" >> "$tmp"

echo 'val elapsed = (new Date().getTime - startTime.getTime) / 100 / 10.0' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
