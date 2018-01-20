sql="$1"
partitions="$2"
decoders="$3"

set -eu

source ./_env.sh

if [ -z "$partitions" ]; then
	partitions="8"
fi
if [ -z "$decoders" ]; then
	decoders="8"
fi

if [ -z "$sql" ]; then
	echo "<bin> usage: <bin> query-sql [partitions=8] [decoders=8]" >&2
	exit 1
fi

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'import java.text.SimpleDateFormat' > "$tmp"
echo 'import java.util.Date' >> "$tmp"

echo 'spark.conf.set("spark.ch.plan.codegen", "'$codegen'")' >> "$tmp"
echo 'spark.conf.set("spark.ch.plan.pushdown.agg", "'$pushdown'")' >> "$tmp"
echo 'val ch = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"

./ch-q.sh "show tables" | while read table; do
	echo "ch.mapCHClusterTable(table=\"$table\", partitions=$partitions, decoders=$decoders)" >> "$tmp"
done

echo 'val startTime = new Date()' >> "$tmp"

echo "ch.sql(\"$sql\").show(false)" >> "$tmp"

echo 'val endTime: Date = new Date()' >> "$tmp"
echo 'val elapsed = endTime.getTime - startTime.getTime' >> "$tmp"
echo 'val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")' >> "$tmp"
echo 'val date = dateFormat.format(elapsed)' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
