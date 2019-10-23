#!/bin/bash

# Which tikv to be stopped
tikv_dir="${1}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"
auto_error_handle

if [ -z "${tikv_dir}" ]; then
	echo "[script tikv_stop] usage: <script> tikv_dir" >&2
	exit 1
fi

tikv_stop "${tikv_dir}"
