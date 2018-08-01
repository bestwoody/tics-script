#!/bin/bash

source _env.sh

cp "$repo_dir/computing/conf/spark-defaults.conf" "$repo_dir/computing/spark/conf/"
if [ "$?" != "0" ]; then
	echo "Copy config file to spark failed." >&2
	exit 1
fi

ip="$spark_master"

if [ -z "$ip" ]; then
	if [ `uname` == "Darwin" ]; then
		ip=`ifconfig | grep -i mask | grep broadcast | grep inet | awk '{print $2}'`
	else
		ip=`ifconfig | grep -i mask | grep cast | grep inet | awk '{print $2}' | awk -F 'addr:' '{print $2}'`
	fi
fi

if [ -z "$ip" ]; then
	ip="127.0.0.1"
fi

echo "master ip: $ip"

$repo_dir/computing/spark/bin/spark-shell --master spark://$ip:7077 --executor-cores $spark_executor_cores --executor-memory $spark_executor_memory $@ 2>&1
