#!/bin/bash

function bash_conf_write()
{
	if [ -z "${3+x}" ]; then
		echo "[func bash_conf_write] usage: <func> file-path key value" >&2
		return 1
	fi

	local file="${1}"
	local key="${2}"
	local value="${3}"

	local tmp="${file}.tmp"
	cat "${file}" | { grep -v "${key}" || test $? = 1; } > "${tmp}"
	echo "export ${key}='"${value}"'" >> "${tmp}"
	mv -f "${tmp}" "${file}"
}
export -f bash_conf_write

function bash_conf_get()
{
	if [ -z "${2+x}" ]; then
		echo "[func bash_conf_get] usage: <func> file-path key" >&2
		return 1
	fi

	local file="${1}"
	local key="${2}"

	local value=`cat "${file}" | { grep "${key}" || test $? = 1; } | awk -F '=' '{print $2}'`
	if [ -z "${value}" ]; then
		echo "(not set)"
	else
		local value=`echo "${value}" | tr -d '"' | tr -d "'"`
		echo "${value}"
	fi
}
export -f bash_conf_get

function cmd_ti_builtin_bin_path()
{
	if [ -z "${1+x}" ]; then
		bash_conf_get "${integrated}/ops/conf.sh" 'DEFAULT_BIN_PATH'
	else
		if [ ! -z "${2+x}" ]; then
			echo "[cmd builtin/bin_path] usage: <cmd> [new_bin_path]" >&2
			return 1
		fi
		local path="${1}"
		if [ ! -d "${path}" ]; then
			echo "[cmd builtin/bin_path] error: ${path} is not a dir" >&2
			return 1
		fi
		local path=`abs_path "${path}"`
		bash_conf_write "${integrated}/ops/conf.sh" 'DEFAULT_BIN_PATH' "${path}"
	fi
}

set -euo pipefail
cmd_ti_builtin_bin_path "${@}"
