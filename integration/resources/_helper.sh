#!/bin/bash

function get_pid()
{
	local file="$1"
	local name="$2"
	local pid=`ps -ef | grep "$bin/$file" | grep "$name" | grep -v grep | awk '{print $2}'`
	if [ -z "$pid" ]; then
		return;
	fi
	local pid_count=`echo "$pid" | wc -l | awk '{print $1}'`
	if [ "$pid_count" != "1" ]; then
		echo "$file $name pid count: $pid_count != 1, do nothing" >&2
		exit 1
	fi
	#echo $file $name $pid | awk '{print $3}'
	echo $pid
}
export -f get_pid

function stop()
{
	local file="$1"
	local name="$2"
	local fast=""
	if [ ! -z ${3+x} ]; then
		fast="$3"
	fi
	#echo "stopping {$file}, fast: {$fast}"
	local pid=`get_pid "$file" "$name"`
	if [ -z "$pid" ]; then
		return;
	fi
	local heavy_kill="false"
	local heaviest_kill="false"

	if [ "$fast" == "true" ]; then
		heaviest_kill="true"
	fi

	set +e
	for ((i=0; i<600; i++)); do
		if [ "$heaviest_kill" == "true" ]; then
			echo "   #$i pid $pid closing, using 'kill -9'..."
			kill -9 $pid
		else
			if [ "$heavy_kill" == "true" ]; then
				echo "   #$i pid $pid closing, using double kill..."
			else
				echo "   #$i pid $pid closing..."
			fi
			kill $pid
			if [ "$heavy_kill" == "true" ]; then
				kill $pid
			fi
		fi

		sleep 1

		pid_exists=`get_pid "$file" "$name"`
		if [ -z "$pid_exists" ]; then
			echo "   #$i pid $pid closed"
			break
		fi

		if [ $i -ge 29 ]; then
			heavy_kill="true"
		fi
		if [ $i -ge 39 ]; then
			heaviest_kill="true"
		fi
		if [ $i -ge 119 ]; then
			echo "   pid $pid close failed" >&2
			exit 1
		fi
	done

	# TODO: restore old setting
	set -e
        set -o pipefail
}
export -f stop


