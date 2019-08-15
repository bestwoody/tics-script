#!/bin/bash

ti_file="${1}"
cmd="${2}"
args="${3}"
conf_templ_dir="${4}"

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

if [ -z "${ti_file}" ]; then
	echo "[script ti.sh] usage: <script> ti_file cmd(run|stop|status|prop|fstop|dry) [args(k=v#k=v:..)] [conf_templ_dir]" >&2
	exit 1
fi

if [ -z "${cmd}" ]; then
	cmd="status"
fi
if [ "${cmd}" == "up" ]; then
	cmd="run"
fi

if [ -z "${conf_templ_dir}" ]; then
	conf_templ_dir="${integrated}/launch/local/conf_templ"
fi

ti_file_exe "${cmd}" "${ti_file}" "${conf_templ_dir}" "${args}"
