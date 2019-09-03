#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1_x.ti"
args="ports=+2#dir=nodes/2"

"${ti}" -k "${args}" "${ti_file}" burn doit

start_time=`date +%s`

"${ti}" -k "${args}" -m pd "${ti_file}" up
"${ti}" -k "${args}" -m tikv "${ti_file}" up
"${ti}" -k "${args}" -m tidb "${ti_file}" up
"${ti}" -k "${args}" -m tiflash "${ti_file}" up
"${ti}" -k "${args}" -m rngine "${ti_file}" up

end_time=`date +%s`

data="${BASH_SOURCE[0]}.data"

procs=`"${ti}" -k "${args}" "${ti_file}"`
ok=`echo "${procs}" | grep 'OK' | wc -l | awk '{print $1}'`
if [ "${ok}" != '5' ]; then
	echo "${procs}"
else
	echo $((end_time - start_time)) >> "${data}"
	tail -n 10 "${data}" | tr '\n' ' ' | awk '{print "launch elapsed(s): "$0}' > "${BASH_SOURCE[0]}.report"
fi

"${ti}" -k "${args}" "${ti_file}" burn doit

echo 'done'
