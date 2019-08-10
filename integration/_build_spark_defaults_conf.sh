#!/bin/bash

set -ue
set -o pipefail

source ./_cluster_env.sh

tiflash_addrs=
for i in ${!storage_server[@]}; do
    if [[ $i != 0 ]]; then
		tiflash_addrs+=,
	fi
	tiflash_addrs+=${storage_server[i]}:${tiflash_tcp_port}
done

cat<<EOF>./resources/spark-defaults.conf
# This is a generated file. Do not edit it. Changes will be overwritten.
## Flash specific configs
spark.tispark.pd.addresses            ${pd_addrs}
spark.flash.addresses                 ${tiflash_addrs}
spark.sql.extensions                  org.apache.spark.sql.CHExtensions
spark.tispark.show_rowid              false
spark.tispark.request.isolation.level SI
spark.storage.partitionsPerSplit      10

## Spark configs
# spark.master                        spark://master:7077
# spark.eventLog.enabled              true
# spark.eventLog.dir                  hdfs://namenode:8021/directory
# spark.serializer                    org.apache.spark.serializer.KryoSerializer
spark.driver.memory                   8g
spark.local.dir                       /data1/tmp
#spark.shuffle.memoryFraction         0.8
spark.shuffle.safetyFraction          0.8
spark.shuffle.spill                   true
spark.driver.extraClassPath           spark/jars/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar
spark.driver.extraJavaOptions         -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=8089 -Djava.rmi.server.hostname=localhost -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:MaxDirectMemorySize=5g -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=5005,suspend=n
EOF