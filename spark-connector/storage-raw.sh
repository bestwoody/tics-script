query="$1"
verb="$3"
host="127.0.0.1"
port="9000"

set -eu

if [ -z "$query" ]; then
    echo "usage: <bin> query [verb=2]" >&2
fi

if [ -z "$verb" ]; then
    verb="2"
fi

java -XX:MaxDirectMemorySize=5g \
    -cp chspark/target/*:chspark/target/lib/*:spark/assembly/target/scala-2.11/jars/* \
    org.apache.spark.sql.ch/CHRawScala \
    "$query" "$verb" "$host" "$port" \
    2>&1 | grep -v 'SLF4J' | grep -v 'log4j'
