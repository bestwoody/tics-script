#!/bin/bash

# Which pd to be stopped
pd_dir="${1}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"
auto_error_handle

if [ -z "${pd_dir}" ]; then
	echo "[script pd_stop] usage: <script> pd_dir" >&2
	exit 1
fi

pd_stop "${pd_dir}"
