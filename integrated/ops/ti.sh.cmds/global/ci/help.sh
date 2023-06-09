#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

if [ -z "${1+x}" ]; then
	ti_file_cmd_list "${here}"
else
	matching="${1}"
	ti_file_cmd_list "${here}" "${1}"
fi
