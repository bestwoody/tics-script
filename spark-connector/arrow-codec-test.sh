set -eu

java \
	-cp ch-sdk/target/*:ch-sdk/target/lib/* \
	org.apache.spark.sql.ch/CHRaw
