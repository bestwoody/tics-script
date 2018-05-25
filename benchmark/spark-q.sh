sql="$1"
partitions="$2"
decoders="$3"
encoders="$4"

set -eu

source ./_env.sh

if [ -z "$partitions" ]; then
	partitions="$default_partitions"
fi
if [ -z "$decoders" ]; then
	decoders="$default_decoders"
fi
if [ -z "$encoders" ]; then
	encoders="$default_encoders"
fi

if [ -z "$sql" ]; then
	echo "<bin> usage: <bin> query-sql [partitions] [decoders] [encoders]" >&2
	exit 1
fi

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'import java.text.SimpleDateFormat' > "$tmp"
echo 'import java.util.Date' >> "$tmp"

echo 'spark.conf.set("spark.ch.plan.pushdown.agg", "'$pushdown'")' >> "$tmp"
echo 'spark.conf.set("spark.ch.plan.single.node.opt", "'$single_node_optimization'")' >> "$tmp"
echo 'spark.conf.set("spark.ch.storage.selraw", "'$selraw'")' >> "$tmp"
echo 'spark.conf.set("spark.ch.storage.tableinfo.selraw", "'$selraw_tableinfo'")' >> "$tmp"

echo 'val storage = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"

./storage-client.sh "show tables" | while read table; do
	echo "storage.mapCHClusterTable(database=\"$storage_db\", table=\"$table\", partitions=$partitions, decoders=$decoders, encoders=$encoders)" >> "$tmp"
done

echo 'val startTime = new Date()' >> "$tmp"

echo "storage.sql(\"$sql\").show(false)" >> "$tmp"

echo 'val elapsed = (new Date().getTime - startTime.getTime) / 100 / 10.0' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
