#!/bin/bash

# Which rngine to be stopped
rngine_dir="${1}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"
auto_error_handle

if [ -z "${rngine_dir}" ]; then
	echo "[script rngine_stop] usage: <script> rngine_dir" >&2
	exit 1
fi

rngine_stop "${rngine_dir}"
