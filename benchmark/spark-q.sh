sql="$1"
tmp="$2"
partitions="$3"
decoders="$4"

set -eu

if [ -z "$partitions" ]; then
	partitions="8"
fi
if [ -z "$decoders" ]; then
	decoders="8"
fi

if [ -z "$sql" ]; then
	echo "<bin> usage: <bin> query-sql [tmp-sql-file]" >&2
	exit 1
fi

if [ -z "$tmp" ]; then
	mkdir -p "/tmp/spark-q/"
	tmp="/tmp/spark-q/`date +%s`"
fi

echo 'import java.text.SimpleDateFormat' > "$tmp"
echo 'import java.util.Date' >> "$tmp"
echo 'val ch = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"

./ch-q.sh "show tables" | while read table; do
	echo "ch.mapCHClusterTable(table=\"$table\", partitions=$partitions, decoders=$decoders)" >> "$tmp"
done

echo 'val startTime = new Date()' >> "$tmp"

echo "ch.sql(\"$sql\").show" >> "$tmp"

echo 'val endTime: Date = new Date()' >> "$tmp"
echo 'val elapsed = endTime.getTime - startTime.getTime' >> "$tmp"
echo 'val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")' >> "$tmp"
echo 'val date = dateFormat.format(elapsed)' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
