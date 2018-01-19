from="$1"
to="$2"
partitions="$3"
decoders="$4"

set -eu

if [ -z "$partitions" ]; then
	partitions="8"
fi

if [ -z "$decoders" ]; then
	decoders="8"
fi

if [ -z "$to" ]; then
	echo "<bin> usage: <bin> from-table-name to-parquet-file-path [reading-partition-count] [reading-decoder-count]" >&2
	exit 1
fi

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"
repartition="16"

echo 'import java.text.SimpleDateFormat' > "$tmp"
echo 'import java.util.Date' >> "$tmp"
echo 'import spark.implicits._'>> "$tmp"
echo 'val ch = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"

./ch-q.sh "show tables" | while read table; do
	echo "ch.mapCHClusterTable(table=\"$table\", partitions=$partitions, decoders=$decoders)" >> "$tmp"
done

echo 'val startTime = new Date()' >> "$tmp"
echo 'spark.sql("select * from '$from'").repartition('$repartition').write.parquet("'$to'")'>> "$tmp"

echo 'val endTime: Date = new Date()' >> "$tmp"
echo 'val elapsed = endTime.getTime - startTime.getTime' >> "$tmp"
echo 'val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")' >> "$tmp"
echo 'val date = dateFormat.format(elapsed)' >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
