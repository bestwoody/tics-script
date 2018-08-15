#!/bin/bash

set -u

source ./_env.sh

master_check=`ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep | wc -l | awk '{print $1}'`
echo "$master_check master(s) is running"

slave_check=`ps -ef | grep org.apache.spark.deploy.worker.Worker | grep -v grep | wc -l | awk '{print $1}'`
echo "$slave_check slave(s) is running"

if [ "$master_check" != "0" ]; then
	# This is for spark 2.3.0
	#slave_in_master_check=`curl -s $spark_master:8080 | grep ' Workers (' | awk -F '(' '{print $2}' | awk -F ')' '{print $1}'`
	slave_in_master_check=`curl -s $spark_master:8080 | grep ALIVE | grep -v Status | wc -l | awk '{print $1}'`
	echo "$slave_in_master_check slave(s) alive in master"
fi
