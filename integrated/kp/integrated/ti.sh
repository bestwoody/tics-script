#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1_x.ti"
args="ports=+3#dir=nodes/3"

"${ti}" -k "${args}" "${ti_file}" burn doit

start_time=`date +%s`

"${ti}" -k "${args}" "${ti_file}" up

ok=`"${ti}" -k "${args}" "${ti_file}" | grep 'OK' | wc -l | awk '{print $1}'`

"${ti}" -k "${args}" "${ti_file}" down

end_time=`date +%s`

if [ "${ok}" != '5' ]; then
	"${ti}" "${ti_file}" >&2
else
	echo $((end_time - start_time)) >> "${BASH_SOURCE[0]}.data"
	tail -n 10 "${BASH_SOURCE[0]}.data" | tr '\n' ' ' | \
		awk '{print "launch+stop elapsed(s): "$0}' > "${BASH_SOURCE[0]}.report"
fi
echo 'done'

"${ti}" -k "${args}" "${ti_file}" burn doit
