#!/bin/bash

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
		echo "using bins in ${path}"
	fi
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
set -euo pipefail
cmd_ti_builtin_bin_path "${@}"
