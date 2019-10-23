#!/bin/bash

path="${1}"

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

if [ -z "${path}" ]; then
	echo "[cmd new] usage: <cmd> file-path-to-be-created.ti"
	exit 1
fi

ext=`print_file_ext "${path}"`
if [ "${ext}" != 'ti' ]; then
	echo "[cmd new] the file name must have '.ti' suffix"
	exit 1
fi

cat "${here}/new.ti" > "${path}"

echo "${path} created"
