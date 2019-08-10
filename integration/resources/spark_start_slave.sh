#!/bin/bash

source ./_env_build.sh

master="$1"
if [ -z "$master" ] && [ ! -z "$spark_master" ]; then
	master="$spark_master"
fi

set -eu

if [ -z "`echo $master | grep ':'`" ]; then
	master="$master:7077"
fi

spark/sbin/start-slave.sh $master
slave_check=`ps -ef | grep org.apache.spark.deploy.worker.Worker | grep -v grep | wc -l | awk '{print $1}'`
if [ "$slave_check" != "1" ]; then
	echo "launch slave failed: $slave_check" >&2
	exit 1
fi
