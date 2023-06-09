#!/bin/bash

source ./_fine-grained-perf-helper.sh

set -u

tmp="/tmp/fine-grained-perf-parquet-`date +%s`.scala"

echo 'import java.io._' >> "$tmp"
echo 'import java.util.Date' >> "$tmp"
echo 'import org.apache.spark.sql.SaveMode' >> "$tmp"
echo 'import org.apache.spark.sql.types._' >> "$tmp"

echo "val tableName = \"${table}\"" >> "$tmp"
echo "val table = spark.read.parquet(\"parquet/${table}3\")" >> "$tmp"
echo 'table.createOrReplaceTempView(tableName)' >> "$tmp"

echo "val logger = new PrintWriter(new File(\"$report\"))" >> "$tmp"
echo 'def log(line: String) = {' >> "$tmp"
echo 'logger.write(line + "\n")' >> "$tmp"
echo 'logger.flush()' >> "$tmp"
echo '}' >> "$tmp"

echo "val gbyThreshold = $gby_threshold" >> "$tmp"

echo 'def time_parquet_q(id: String, q: String) = {' >> "$tmp"
echo 'log(id + " query: " + q)' >> "$tmp"
echo 'val startTime = new Date()' >> "$tmp"
echo 'try {' >> "$tmp"
echo 'spark.sql(q).write.format("org.apache.spark.sql.NullFileFormat").mode(SaveMode.Overwrite).save("/tmp/nullio")' >> "$tmp"
echo 'val elapsed = (new Date().getTime - startTime.getTime) / 100 / 10.0' >> "$tmp"
echo 'log(elapsed.toString())' >> "$tmp"
echo '} catch {' >> "$tmp"
echo 'case t: Throwable => log(t.getMessage())' >> "$tmp"
echo 't.printStackTrace()' >> "$tmp"
echo '}' >> "$tmp"
echo '}' >> "$tmp"

echo 'def result_parquet_q(q: String): String = {' >> "$tmp"
echo 'try {' >> "$tmp"
echo 'return spark.sql(q).first.get(0).toString()' >> "$tmp"
echo '} catch {' >> "$tmp"
echo 'case t: Throwable => log(t.getMessage())' >> "$tmp"
echo 't.printStackTrace()' >> "$tmp"
echo 'return ""' >> "$tmp"
echo '}' >> "$tmp"
echo '}' >> "$tmp"

echo 'log("Parquet")' >> "$tmp"
echo 'log("# Fine grained perf report for table " + tableName)' >> "$tmp"

if [ "$skip_single_column" != true ]; then
  echo 'log("## Single column")' >> "$tmp"
  echo 'table.schema.zipWithIndex.foreach {' >> "$tmp"
  echo 'case (field, i) =>' >> "$tmp"
  echo 'log("### Column " + field.name)' >> "$tmp"
  echo 'log("* Type: " + field.dataType)' >> "$tmp"
  echo 'var colCount = result_parquet_q("select count(" + field.name + ") from " + tableName)' >> "$tmp"
  echo 'log("* Count: " + colCount)' >> "$tmp"
  echo 'var colMin = result_parquet_q("select min(" + field.name + ") from " + tableName)' >> "$tmp"
  echo 'log("* Min: " + colMin)' >> "$tmp"
  echo 'var colMax = result_parquet_q("select max(" + field.name + ") from " + tableName)' >> "$tmp"
  echo 'log("* Max: " + colMax)' >> "$tmp"
  echo 'log("<BEGIN_TABLE>")' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "1", "select " + field.name + " from " + tableName)' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "2", "select count(" + field.name + ") from " + tableName)' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "3", "select min(" + field.name + ") from " + tableName)' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "4", "select max(" + field.name + ") from " + tableName)' >> "$tmp"
  echo 'if (field.dataType.isInstanceOf[NumericType]) {' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "5", "select sum(" + field.name + ") from " + tableName)' >> "$tmp"
  echo '}' >> "$tmp"
  echo 'if (field.dataType == StringType) {' >> "$tmp"
  echo 'colMin = "\"" + colMin + "\"" ' >> "$tmp"
  echo 'colMax = "\"" + colMax + "\"" ' >> "$tmp"
  echo '} else if (field.dataType == DateType) {' >> "$tmp"
  echo 'colMin = "date \"" + colMin + "\"" ' >> "$tmp"
  echo 'colMax = "date \"" + colMax + "\"" ' >> "$tmp"
  echo '}' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "6", "select " + field.name + " from " + tableName + " where " + field.name + " >= " + colMin + " and " + field.name + " <= " + colMax)' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "7", "select " + field.name + " from " + tableName + " where " + field.name + " < " + colMin + " or " + field.name + " > " + colMax)' >> "$tmp"
  echo 'if (field.dataType == StringType) {' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "8", "select " + field.name + " = \"abc\" from " + tableName)' >> "$tmp"
  echo '} else if (field.dataType == DateType) {' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "9", "select " + field.name + " + interval 1 day > date \"1990-01-01\" from " + tableName)' >> "$tmp"
  echo '} else {' >> "$tmp"
  echo 'time_parquet_q("SC" + i + "10", "select " + field.name + " + 1 > 10000 from " + tableName)' >> "$tmp"
  echo '}' >> "$tmp"
  echo 'log("<END_TABLE>")' >> "$tmp"
  echo '}' >> "$tmp"
fi

if [ "$skip_multi_column" != true ]; then
  echo 'log("## Multi columns")' >> "$tmp"
  echo 'for (i <-0 until table.schema.fields.length) {' >> "$tmp"
  echo 'log("<BEGIN_TABLE>")' >> "$tmp"
  echo 'val proj = table.schema.fields.slice(0, i + 1).map(_.name).mkString(", ")' >> "$tmp"
  echo 'val projCount = table.schema.fields.slice(0, i + 1).map("count(" + _.name + ")").mkString(", ")' >> "$tmp"
  echo 'val projMin = table.schema.fields.slice(0, i + 1).map("min(" + _.name + ")").mkString(", ")' >> "$tmp"
  echo 'val projMax = table.schema.fields.slice(0, i + 1).map("max(" + _.name + ")").mkString(", ")' >> "$tmp"
  echo 'time_parquet_q("MC" + i + "1", "select " + proj + " from " + tableName)' >> "$tmp"
  echo 'time_parquet_q("MC" + i + "2", "select " + projCount + " from " + tableName)' >> "$tmp"
  echo 'time_parquet_q("MC" + i + "3", "select " + projMin + " from " + tableName)' >> "$tmp"
  echo 'time_parquet_q("MC" + i + "4", "select " + projMax + " from " + tableName)' >> "$tmp"
  echo 'log("<END_TABLE>")' >> "$tmp"
  echo '}' >> "$tmp"
fi

if [ "$skip_gby" != true ]; then
  echo 'log("## Group by")' >> "$tmp"
  echo 'table.schema.zipWithIndex.foreach {' >> "$tmp"
  echo 'case (field, i) =>' >> "$tmp"
  echo 'try {' >> "$tmp"
  echo 'val dc = result_parquet_q("select count(distinct " + field.name + ") from " + tableName).toLong' >> "$tmp"
  echo 'if (dc <= gbyThreshold) {' >> "$tmp"
  echo 'log("### Column " + field.name)' >> "$tmp"
  echo 'log("* Distinct count: " + dc)' >> "$tmp"
  echo 'log("<BEGIN_TABLE>")' >> "$tmp"
  echo 'time_parquet_q("GB" + i + "1", "select count(*) from " + tableName + " group by " + field.name)' >> "$tmp"
  echo 'log("<END_TABLE>")' >> "$tmp"
  echo '}' >> "$tmp"
  echo '} catch {' >> "$tmp"
  echo 'case t: Throwable =>' >> "$tmp"
  echo 't.printStackTrace()' >> "$tmp"
  echo '}' >> "$tmp"
  echo '}' >> "$tmp"
fi

echo 'logger.close()' >> "$tmp"

./spark-shell.sh < "$tmp"
