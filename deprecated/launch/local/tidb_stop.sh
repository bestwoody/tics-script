#!/bin/bash

# Which tidb to be stopped
tidb_dir="${1}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"
auto_error_handle

if [ -z "${tidb_dir}" ]; then
	echo "[script tidb_stop] usage: <script> tidb_dir" >&2
	exit 1
fi

tidb_stop "${tidb_dir}"
