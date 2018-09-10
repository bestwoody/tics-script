#!/bin/bash

set -eu

name="spark-2.3.1-bin-hadoop2.7"
address="http://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-2.3.1"

file="$name.tgz"
url="$address/$file"

echo "=> downloading $file"
rm -f "$file"
wget "$url"

echo "=> unpacking $file"
if [ -d "./spark" ]; then
	if [ -d "./spark.old" ]; then
		echo "dir 'spark' and 'spark.old' exists, can't backup, exiting..." >&2
		exit 1
	fi
	mv ./spark ./spark.old
fi
tar -xvzf "$file"

echo "=> remove $file"
rm -f "$file"

echo "=> mv $name to spark"
mv "$name" spark

echo "OK"
