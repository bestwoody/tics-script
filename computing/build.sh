#!/bin/bash

set -eu

old=`pwd`

cd chspark
mvn clean package -DskipTests
cd "$old"

cp "chspark/target/chspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar" "spark/jars/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar"

echo
echo "OK"
