#!/bin/bash

spark/sbin/stop-master.sh
spark/sbin/stop-slave.sh

sleep 1

set -eu

pid_count()
{
	local trait="$1"
	ps -ef | grep "$trait" | grep -v grep | wc -l | awk '{print $1}'
}

print_pid()
{
	local trait="$1"
	ps -ef | grep "$trait" | grep -v grep | awk '{print $2}'
}

kill_daemon()
{
	local name="$1"
	local trait="$2"

	local n="`pid_count $trait`"
	if [ "$n" == "0" ]; then
		return
	fi

	local pid=`print_pid "$trait"`
	echo "stop $name(pid: $pid) failed, using kill" >&2
	kill "$pid"

	sleep 1

	n="`pid_count $trait`"
	if [ "$n" == "0" ]; then
		return
	fi

	echo "stop $name failed, using kill -9" >&2
	kill -9 "$pid"

	sleep 1

	n="`pid_count $trait`"
	if [ "$n" != "0" ]; then
		echo "stop $name failed, exiting ..." >&2
	fi
}

kill_daemon "Spark master" "org.apache.spark.deploy.master.Master"
kill_daemon "Spark worker" "org.apache.spark.deploy.worker.Worker"

echo
echo "OK"
