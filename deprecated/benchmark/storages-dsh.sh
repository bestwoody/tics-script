#!/bin/bash

cmd="$1"
async="$2"
user="$3"

set -eu

source ./_env.sh

if [ -z "$cmd" ]; then
	echo "usage: <bin> cmd [async=false] [user=whoami]" >&2
	exit 1
fi
cmd="cd `pwd`; $cmd"

if [ -z "$async" ]; then
	async="false"
fi

if [ ! -z "$user" ]; then
	user="$user@"
fi

get_hosts()
{
	for server in ${storage_server[@]}; do
		echo `get_host $server`
	done
}

execute_cmd()
{
	local host="$1"
	ssh ${user}${host} $cmd < /dev/null 2>&1 | while read line; do
		echo [$host] $line
	done
}

get_hosts | sort | uniq | while read host; do
	if [ "$async" == "true" ]; then
		execute_cmd "$host" &
	else
		execute_cmd "$host"
	fi
done

wait
