#!/bin/bash

function ti_file_mod_status()
{
	if [ -z "${2+x}" ]; then
		echo "[func ti_file_mod_status] usage <func> dir conf" >&2
		return 1
	fi

	local dir="${1}"
	local conf="${2}"

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
	echo "${up_status}"
}
export -f ti_file_mod_status

function ti_file_cmd_status()
{
	if [ -z "${4+x}" ]; then
		echo "[func ti_file_mod_status] usage <func> index name dir conf" >&2
		return 1
	fi

	local index="${1}"
	local name="${2}"
	local dir="${3}"
	local conf="${4}"

	local up_status=`ti_file_mod_status "${dir}" "${conf}"`
	echo "${up_status} ${name} #${index} (${dir})"
}
export -f ti_file_cmd_status

function ti_file_mod_stop()
{
	if [ -z "${2+x}" ]; then
		echo "[func ti_file_mod_stop] usage <func> index name dir conf fast_mode" >&2
		return 1
	fi

	local index="${1}"
	local name="${2}"
	local dir="${3}"
	local conf="${4}"
	local fast="${5}"

	local up_status=`ti_file_mod_status "${dir}" "${conf}"`
	local ok=`echo "${up_status}" | grep ^OK`
	if [ -z "${ok}" ]; then
		echo "=> skipped. ${name} #${index} (${dir}) ${up_status}"
		return
	fi

	echo "=> stopping ${name} #${index} (${dir})"

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

	local up_status=`ti_file_mod_status "${dir}" "${conf}"`
	local ok=`echo "${up_status}" | grep ^OK`
	if [ ! -z "${ok}" ]; then
		echo "failed.. ${name} #${index} (${dir}) ${up_status}"
		return 1
	fi
}
export -f ti_file_mod_stop

function ti_file_cmd_stop()
{
	ti_file_mod_stop "${1}" "${2}" "${3}" "${4}" 'false'
}
export -f ti_file_cmd_stop

function ti_file_cmd_fstop()
{
	ti_file_mod_stop "${1}" "${2}" "${3}" "${4}" 'true'
}
export -f ti_file_cmd_fstop

function ti_file_exe()
{
	local help="[func ti_file_exe] usage: <func> cmd ti_file conf_templ_dir cmd_dir [ti_file_args(k=v#k=v#..)] [mod_names] [hosts] [byhost] [cmd_args...]"
	if [ -z "${3+x}" ]; then
		echo "${help}" >&2
		return 1
	fi

	local cmd="${1}"
	local ti_file="${2}"
	local conf_templ_dir="${3}"
	local cmd_dir="${4}"

	if [ -z "${5+x}" ]; then
		local ti_args=""
	else
		local ti_args="${5}"
	fi

	if [ -z "${6+x}" ]; then
		local mod_names=""
	else
		local mod_names="${6}"
	fi

	if [ -z "${7+x}" ]; then
		local hosts=""
	else
		local hosts="${7}"
	fi

	if [ -z "${8+x}" ]; then
		local byhost=""
	else
		local byhosts="${8}"
	fi

	shift 8
	local cmd_args=("${@}")

	if [ ! -f "${ti_file}" ]; then
		if [ -d "${ti_file}" ]; then
			echo "[func ti_file_exe] '${ti_file}' is dir, not a file"
		else
			echo "[func ti_file_exe] '${ti_file}' not exists"
		fi
		return 1
	fi

	# For ti file checking
	python "${integrated}/_base/ti_file.py" 'hosts' \
		"${ti_file}" "${integrated}" "${conf_templ_dir}" "${mod_names}" "${hosts}" "${ti_args}" 1>/dev/null

	local hosts=`python "${integrated}/_base/ti_file.py" 'hosts' \
		"${ti_file}" "${integrated}" "${conf_templ_dir}" "${mod_names}" "${hosts}" "${ti_args}"`

	# TODO: From arg
	local cache_root="/tmp/ti/integrated"
	local remote_env="${cache_root}/worker/integrated"

	# TODO: Parallel ping and copy
	echo "${hosts}" | while read host; do
		if [ ! -z "${host}" ]; then
			ssh_ping "${host}"
		fi
	done
	echo "${hosts}" | while read host; do
		if [ ! -z "${host}" ]; then
			cp_env_to_host "${integrated}" "${cache_root}/master/integrated" "${host}" "${cache_root}/worker"
		fi
	done

	if [ "${byhost}" != "true" ]; then
		if [ "${cmd}" == "run" ] || [ "${cmd}" == "dry" ]; then
			python "${integrated}/_base/ti_file.py" 'render' "${ti_file}" \
				"${integrated}" "${conf_templ_dir}" "${mod_names}" "${hosts}" "${ti_args}" > "${ti_file}.sh"
			chmod +x "${ti_file}.sh"
			if [ "${cmd}" == "run" ]; then
				bash "${ti_file}.sh"
				rm -f "${ti_file}.sh"
			fi
			return 0
		fi

		local mods=`python "${integrated}/_base/ti_file.py" 'mods' \
			"${ti_file}" "${integrated}" "${conf_templ_dir}" "${mod_names}" "${hosts}" "${ti_args}"`

		local has_func=`func_exists "ti_file_cmd_${cmd}"`
		local has_script='false'
		if [ -f "${cmd_dir}/${cmd}.sh" ]; then
			local has_script='true'
		fi

		echo "${mods}" | while read mod; do
			if [ -z "${mod}" ]; then
				continue
			fi
			local index=`echo "${mod}" | awk -F '\t' '{print $1}'`
			local name=`echo "${mod}" | awk -F '\t' '{print $2}'`
			local dir=`echo "${mod}" | awk -F '\t' '{print $3}'`
			local conf=`echo "${mod}" | awk -F '\t' '{print $4}'`
			local host=`echo "${mod}" | awk -F '\t' '{print $5}'`
			if [ "${has_script}" == 'true' ]; then
				if [ -z "${host}" ]; then
					if [ -z "${cmd_args+x}" ]; then
						bash "${cmd_dir}/${cmd}.sh" "${index}" "${name}" "${dir}" "${conf}"
					else
						bash "${cmd_dir}/${cmd}.sh" "${index}" "${name}" "${dir}" "${conf}" "${cmd_args[@]}"
					fi
				else
					if [ -z "${cmd_args+x}" ]; then
						call_remote_func "${host}" "${remote_env}" script_exe "${cmd_dir}/${cmd}.sh" \
							"${index}" "${name}" "${dir}" "${conf}"
					else
						call_remote_func "${host}" "${remote_env}" script_exe "${cmd_dir}/${cmd}.sh" \
							"${index}" "${name}" "${dir}" "${conf}" "${cmd_args[@]}"
					fi
				fi
				continue
			fi
			if [ "${has_func}" == 'true' ]; then
				if [ -z "${host}" ]; then
					if [ -z "${cmd_args+x}" ]; then
						"ti_file_cmd_${cmd}" "${index}" "${name}" "${dir}" "${conf}"
					else
						"ti_file_cmd_${cmd}" "${index}" "${name}" "${dir}" "${conf}" "${cmd_args[@]}"
					fi
				else
					if [ -z "${cmd_args+x}" ]; then
						call_remote_func "${host}" "${remote_env}" "ti_file_cmd_${cmd}" "${index}" "${name}" \
							"${dir}" "${conf}"
					else
						call_remote_func "${host}" "${remote_env}" "ti_file_cmd_${cmd}" "${index}" "${name}" \
							"${dir}" "${conf}" "${cmd_args[@]}"
					fi
				fi
				continue
			fi
		done
		if [ "${has_func}" != 'true' ] && [ "${has_script}" != 'true' ]; then
			echo "script not found: ${cmd_dir}/${cmd}.sh"
			return 1
		fi
	else
		if [ -f "${cmd_dir}/bynode/${cmd}.sh" ]; then
			echo "${hosts}" | while read host; do
				if [ -z "${host}" ]; then
					continue
				fi
				if [ -z "${cmd_args+x}" ]; then
					call_remote_func "${host}" "${remote_env}" script_exe "${cmd_dir}/bynode/${cmd}.sh" "${host}"
				else
					call_remote_func "${host}" "${remote_env}" script_exe "${cmd_dir}/bynode/${cmd}.sh" \
						"${host}" "${cmd_args[@]}"
				fi
			done
		else
			echo "script not found: ${cmd_dir}/bynode/${cmd}.sh"
			return 1
		fi
	fi
}
export -f ti_file_exe
