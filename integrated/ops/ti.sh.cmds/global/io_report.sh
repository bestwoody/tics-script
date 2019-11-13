#!/bin/bash

file="${1}"

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

if [ -z "${file}" ]; then
	file="./fio.tmp"
fi

log="${file}.log"
rm -f "${log}"

fio_report "${file}" "${log}"
