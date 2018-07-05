set -eu

source ./_env.sh

if [ -z "${partitionsPerSplit+x}" ]; then
	partitionsPerSplit="$default_partitionsPerSplit"
fi

if [ "$#" -lt 1 ]; then
	echo "<bin> usage: <bin> q1 q2 ... " >&2
	exit 1
fi

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

spart_settings "$tmp"

echo 'val storage = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"

for sql in "$@"; do
	if [ ! -z "$sql" ]; then
		echo "storage.sql(\"$sql\")" >> "$tmp"
	fi
done

./spark-shell.sh < "$tmp"

rm -f "$tmp"
