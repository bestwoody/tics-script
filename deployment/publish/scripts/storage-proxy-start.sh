#!/bin/bash

daemon_mode="$1"

set -eu
source ./_env.sh

pid=`./storage-proxy-pid.sh`
if [ ! -z "$pid" ]; then
	echo "another tiflash-proxy server is running, skipped and exiting" >&2
	exit 1
fi

export RUST_BACKTRACE=1

export TZ=${TZ:-/etc/localtime}

export GRPC_VERBOSITY=DEBUG
#export GRPC_TRACE=all


if [ "$daemon_mode" != "false" ]; then
	$tiflash_proxy_bin --config "$tiflash_proxy_config" 2>/dev/null &
	sleep 2
	./storage-proxy-pid.sh
else
	$tiflash_proxy_bin --config "$tiflash_proxy_config" 2>/dev/null &
fi
