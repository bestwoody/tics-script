#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"

file="${1}"
if [ -z "${file}" ]; then
	echo "[script kp_watch.sh] usage: <script> kp_file [watch_interval=5]" >&2
	exit 1
fi

interval="${2}"
if [ -z "${interval}" ]; then
	interval=5
fi

width="${3}"
if [ -z "${width}" ]; then
	width=120
fi

watch -c -n "${interval}" -t "COLUMNS= ${integrated}/ops/kp.sh \"${file}\" status \"${width}\""
