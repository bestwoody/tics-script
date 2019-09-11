#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"

file="${1}"
cmd="${2}"

if [ "${cmd}" == 'watch' ]; then
	"`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/kp_watch.sh" "${file}"
else
	cmd_kp "${@}"
fi
