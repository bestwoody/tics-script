#!/bin/bash

path="${1}"

if [ -z "${path}" ]; then
	echo "usage: <cmd> bin_and_conf_path" >&2
	exit 1
fi

set -euo pipefail

function cp_to_conf()
{
	local path="${1}"
	local name="${2}"

	if [ -d "${path}/${name}" ]; then
		local files="${path}/${name}"
	else
		local files=`ls "${path}/"${name} 2>/dev/null`
	fi
	if [ -z "${files}" ]; then
		return
	fi

	local self_path=`ti.sh builtin/self_path`
	echo "${files}" | while read file; do
		echo cp -r "${file}" "${self_path}/conf/"
		cp -r "${file}" "`ti.sh builtin/self_path`/conf/"
	done
}

ti.sh builtin/bin_path "${path}"

cp_to_conf "${path}" "*.toml"
cp_to_conf "${path}" "*.conf"
cp_to_conf "${path}" "*.kv"
cp_to_conf "${path}" "bin.*"
cp_to_conf "${path}" "default.*"
cp_to_conf "${path}" "tiflash"
