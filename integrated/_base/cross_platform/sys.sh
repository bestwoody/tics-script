#!/bin/bash

function hw_info()
{
	if [ `uname` == "Darwin" ]; then
		echo "This script is for Linux only, not for Mac OS, skipped" >&2
		return 1
	fi

	local cores=`cat /proc/cpuinfo | grep 'core id' | wc -l`
	local model=`cat /proc/cpuinfo | grep 'model name' | head -n 1 | awk -F ': ' '{print $2}'`
	local cpu_hz=`cat /proc/cpuinfo | grep 'cpu MHz' | head -n 1 | awk -F ': ' '{print $2}'`

	local mem=`free -h | grep Mem | awk '{print $2}'`
	local mem_hz=`sudo dmidecode -t memory | grep -i Speed | awk '{print $2}' | sort | uniq | head -n 1`
	if [ ! -z "${mem_hz}" ]; then
		local mem_hz=" @ ${mem_hz} MHz"
	fi

	echo "CPU: ${cores} Cores, ${model} @ ${cpu_hz} MHz"
	echo "Mem: ${mem}${mem_hz}"

	local error_handle="$-"
	set +e
	local rpm_v=`rpm --version 2>/dev/null`
	restore_error_handle_flags "${error_handle}"

	if [ ! -z "${rpm_v}" ]; then
		echo "Sys: `rpm -q centos-release`"
	else
		echo "Sys: `uname -a`"
	fi
}
export -f hw_info
