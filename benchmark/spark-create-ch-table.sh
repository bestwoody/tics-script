set -eu

source ./_env.sh

if [ "$#" -lt 5 ]; then
	echo "<bin> usage: <bin> spark_local_db spark_local_table storage_db storage_table primary_keys" >&2
	exit 1
fi

spark_local_db=$1
spark_local_table=$2
storage_db=$3
storage_table=$4
primary_keys=$5

mkdir -p "/tmp/spark-q/"
tmp="/tmp/spark-q/`date +%s`"

echo 'import org.apache.spark.sql.SparkUtil' >> "$tmp"

spart_settings "$tmp"

echo 'val storage = new org.apache.spark.sql.CHContext(spark)' >> "$tmp"
echo "storage.createDatabase(\"$storage_db\")" >> "$tmp"
echo "storage.dropTable(\"$storage_db\", \"$storage_table\")" >> "$tmp"
echo "var df = storage.sql(\"select * from ${spark_local_db}.${spark_local_table}\")" >> "$tmp"
echo "df = SparkUtil.setNullableStateOfColumns(df, false, Array(${primary_keys}))" >> "$tmp"
echo "storage.createTableFromDataFrame(\"$storage_db\", \"$storage_table\", Array(${primary_keys}), df)" >> "$tmp"
cat $tmp

./spark-shell.sh < "$tmp"

rm -f "$tmp"
