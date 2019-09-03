#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1.ti"
port="ports=+3"

"${ti}" "${ti_file}" burn doit

start_time=`date +%s`

"${ti}" -k "${port}" "${ti_file}" up

ok=`"${ti}" "${ti_file}" | grep 'OK' | wc -l | awk '{print $1}'`

"${ti}" -k "${port}" "${ti_file}" down

end_time=`date +%s`

if [ "${ok}" != '5' ]; then
	"${ti}" "${ti_file}" >&2
else
	echo "all up and down in "$((end_time - start_time))"s" > "${BASH_SOURCE[0]}.report"
fi

"${ti}" "${ti_file}" burn doit
