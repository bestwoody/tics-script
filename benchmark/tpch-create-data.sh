#!/bin/sh
echo "Please enter the generated data size"
echo "Usage: <bin> [ 1 | 10 | 100 | GB]"
if [ ! -n "$1" ]; then
        echo "usage: Please enter the generated N data size" >&2
        exit 1
fi
cp -rf conf/makefile tpch-dbgen/
cp -rf conf/tpcd.h tpch-dbgen/
cd tpch-dbgen
make clean
rm -rf ./*.tbl
make
./dbgen -s $1

