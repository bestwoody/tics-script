set -eu

source ./_env.sh

if [ "$#" -lt 1 ]; then
	echo "<bin> usage: <bin> database" >&2
	exit 1
fi

database="$1"

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'val storage = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"
echo "storage.dropDatabase(\"$database\")" >> "$tmp"

./spark-shell.sh < "$tmp"

rm -f "$tmp"
