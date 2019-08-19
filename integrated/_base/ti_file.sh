#!/bin/bash

function ti_file_prop()
{
	if [ -z "${1+x}" ]; then
		echo "[func ti_file_prop] usage <func> ti_module_info" >&2
		return 1
	fi

	local info="${1}"
	local index=`echo "${info}" | awk '{print $1}'`
	local name=`echo "${info}" | awk '{print $2}'`
	local dir=`echo "${info}" | awk '{print $3}'`

	echo "${name} #${index} (${dir})"
	if [ -f "${dir}/proc.info" ]; then
		cat "${dir}/proc.info" | awk '{print "    "$1": "$2}'
	else
		echo "    MISSED"
	fi
}
export -f ti_file_prop

function ti_file_status()
{
	if [ -z "${1+x}" ]; then
		echo "[func ti_file_status] usage <func> ti_module_info" >&2
		return 1
	fi

	local info="${1}"
	local index=`echo "${info}" | awk '{print $1}'`
	local name=`echo "${info}" | awk '{print $2}'`
	local dir=`echo "${info}" | awk '{print $3}'`
	local conf=`echo "${info}" | awk '{print $4}'`

	local up_status="OK    "
	if [ ! -d "${dir}" ]; then
		local up_status="MISSED"
	else
		local conf_file=`abs_path "${dir}"`/${conf}
		local pid_cnt=`print_proc_cnt "${conf_file}"`
		if [ "${pid_cnt}" == "0" ]; then
			local up_status="DOWN  "
		else
			if [ "${pid_cnt}" != "1" ]; then
				local up_status="MULTI "
			fi
		fi
	fi
	echo "${up_status} ${name} #${index} (${dir})"
}
export -f ti_file_status

function ti_file_stop()
{
	if [ -z "${2+x}" ]; then
		echo "[func ti_file_stop] usage <func> ti_module_info fast_mode" >&2
		return 1
	fi

	local info="${1}"
	local fast="${2}"

	local up_status=`ti_file_status "${info}"`
	local ok=`echo "${up_status}" | grep ^OK`
	if [ -z "${ok}" ]; then
		echo "${up_status}" | awk '{print "=> skipped. "$2,$3,$4,$1}'
		return
	fi

	echo "${up_status}" | awk '{print "=> stopping "$2,$3,$4}'

	local name=`echo "${info}" | awk '{print $2}'`
	local dir=`echo "${info}" | awk '{print $3}'`
	if [ "${name}" == "pd" ]; then
		pd_stop "${dir}" "${fast}"
	fi
	if [ "${name}" == "tikv" ]; then
		tikv_stop "${dir}" "${fast}"
	fi
	if [ "${name}" == "tidb" ]; then
		tidb_stop "${dir}" "${fast}"
	fi
	if [ "${name}" == "tiflash" ]; then
		tiflash_stop "${dir}" "${fast}"
	fi
	if [ "${name}" == "rngine" ]; then
		rngine_stop "${dir}" "${fast}"
	fi

	local up_status=`ti_file_status "${info}"`
	local down=`echo "${up_status}" | grep ^DOWN`
	if [ -z "${down}" ]; then
		echo "${up_status}" | awk '{print "=> failed.. "$2,$3,$4,$1}'
		return 1
	fi
}
export -f ti_file_stop

function ti_file_exe()
{
	local help="[func ti_file_exe] usage: <func> cmd(run|stop|status|prop|fstop|dry) ti_file conf_templ_dir [ti_file_args(k=v#k=v#..)] [mod_names] [hosts] [cmd_args...]"
	if [ -z "${3+x}" ]; then
		echo "${help}" >&2
		return 1
	fi

	local cmd="${1}"
	local ti_file="${2}"
	local conf_templ_dir="${3}"

	if [ -z "${4+x}" ]; then
		local ti_args=""
	else
		local ti_args="${4}"
	fi

	if [ -z "${5+x}" ]; then
		local mod_names=""
	else
		local mod_names="${5}"
	fi

	if [ -z "${6+x}" ]; then
		local hosts=""
	else
		local hosts="${6}"
	fi

	shift 6
	local cmd_args=("${@}")

	if [ ! -f "${ti_file}" ]; then
		if [ -d "${ti_file}" ]; then
			echo "[func ti_file_exe] '${ti_file}' is dir, not a file"
		else
			echo "[func ti_file_exe] '${ti_file}' not exists"
		fi
		return 1
	fi

	if [ "$cmd" == "run" ] || [ "$cmd" == "dry" ]; then
		python "${integrated}/_base/ti_file.py" 'render' "${ti_file}" \
			"${integrated}" "${conf_templ_dir}" "${mod_names}" "${hosts}" "${ti_args}" > "${ti_file}.sh"
		chmod +x "${ti_file}.sh"
		if [ "$cmd" == "run" ]; then
			bash "${ti_file}.sh"
			rm -f "${ti_file}.sh"
		fi
		return 0
	fi

	if [ "${mod_names}" == "hosts" ]; then
		local hosts=`python "${integrated}/_base/ti_file.py" 'hosts' \
			"${ti_file}" "${integrated}" "${conf_templ_dir}" "${mod_names}" "${hosts}" "${ti_args}"`
	else
		local mods=`python "${integrated}/_base/ti_file.py" 'mods' \
			"${ti_file}" "${integrated}" "${conf_templ_dir}" "${mod_names}" "${hosts}" "${ti_args}"`
	fi

	if [ "$cmd" == "prop" ]; then
		echo "${mods}" | while read mod; do
			ti_file_prop "${mod}"
		done
		return
	fi

	if [ "$cmd" == "status" ]; then
		echo "${mods}" | while read mod; do
			ti_file_status "${mod}"
		done
		return
	fi

	if [ "$cmd" == "stop" ] || [ "$cmd" == "fstop" ]; then
		echo "${mods}" | tac | while read mod; do
			if [ "$cmd" == "fstop" ]; then
				ti_file_stop "${mod}" "true"
			else
				ti_file_stop "${mod}" "false"
			fi
		done
		return
	fi

	echo "${help}" >&2
	return 1
}
export -f ti_file_exe
