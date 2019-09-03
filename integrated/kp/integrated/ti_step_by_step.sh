#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1_x.ti"
port="ports=+4"

"${ti}" "${ti_file}" burn doit

start_time=`date +%s`

"${ti}" -k "${port}" -m pd "${ti_file}" up
"${ti}" -k "${port}" -m tikv "${ti_file}" up
"${ti}" -k "${port}" -m tidb "${ti_file}" up
"${ti}" -k "${port}" -m tiflash "${ti_file}" up
"${ti}" -k "${port}" -m rngine "${ti_file}" up

end_time=`date +%s`

ok=`"${ti}" "${ti_file}" | grep 'OK' | wc -l | awk '{print $1}'`
if [ "${ok}" != '5' ]; then
	"${ti}" "${ti_file}" >&2
else
	echo "all up in "$((end_time - start_time))"s" > "${BASH_SOURCE[0]}.report"
fi

"${ti}" "${ti_file}" burn doit
