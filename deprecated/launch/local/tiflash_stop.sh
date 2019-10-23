#!/bin/bash

# Which tiflash to be stopped
tiflash_dir="${1}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"
auto_error_handle

if [ -z "${tiflash_dir}" ]; then
	echo "[script tiflash_stop] usage: <script> tiflash_dir" >&2
	exit 1
fi

tiflash_stop "${tiflash_dir}"
