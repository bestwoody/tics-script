#!/bin/bash

function get_tiflash_addr_from_dir()
{
	if [ -z "${1+x}" ]; then
		echo "[func get_tiflash_addr_from_dir] usage: <func> tiflash_dir" >&2
		return 1
	fi
	local dir="${1}"
	local host=`cat "${dir}/proc.info" | { grep 'listen_host' || test $? = 1; } | awk -F '\t' '{print $2}'`
	local port=`cat "${dir}/proc.info" | { grep 'raft_and_cop_port' || test $? = 1; } | awk -F '\t' '{print $2}'`
	echo "${host}:${port}"
}
export -f get_tiflash_addr_from_dir

function ti_file_exe()
{
	local help="[func ti_file_exe] usage: <func> cmd ti_file conf_templ_dir cmd_dir [ti_file_args(k=v#k=v#..)] [mod_names] [hosts] [byhost] [local] [cache_dir] [cmd_args...]"
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
		local cmd_hosts=""
	else
		local cmd_hosts="${7}"
	fi

	if [ -z "${8+x}" ]; then
		local indexes=""
	else
		local indexes="${8}"
	fi

	if [ -z "${9+x}" ]; then
		local byhost=""
	else
		local byhosts="${9}"
	fi

	if [ -z "${10+x}" ]; then
		local local=""
	else
		local local="${10}"
	fi

	if [ -z "${11+x}" ]; then
		local cache_dir="/tmp/ti"
	else
		local cache_dir="${11}"
	fi

	shift 11

	local error_handle="$-"
	set +u
	local cmd_args=("${@}")
	restore_error_handle_flags "${error_handle}"

	if [ ! -f "${ti_file}" ]; then
		if [ -d "${ti_file}" ]; then
			echo "[func ti_file_exe] '${ti_file}' is dir, not a file" >&2
		else
			echo "[func ti_file_exe] '${ti_file}' not exists" >&2
		fi
		return 1
	fi

	local here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"

	# For ti file checking
	python "${here}/ti_file.py" 'hosts' \
		"${ti_file}" "${integrated}" "${conf_templ_dir}" "${cache_dir}" "${mod_names}" "${cmd_hosts}" "${indexes}" "${ti_args}" 1>/dev/null

	local hosts=`python "${here}/ti_file.py" 'hosts' \
		"${ti_file}" "${integrated}" "${conf_templ_dir}" "${cache_dir}" "${mod_names}" "${cmd_hosts}" "${indexes}" "${ti_args}"`

	# TODO: Pass paths from args
	local remote_env_rel_dir='worker'
	local local_cache_env="${cache_dir}/master/integrated"

	local remote_env_parent="${cache_dir}/${remote_env_rel_dir}"
	local remote_env="${remote_env_parent}/`basename ${local_cache_env}`"

	# TODO: Pass paths from args
	local real_cmd_dir="${cmd_dir}/local"
	if [ "${local}" != 'true' ]; then
		local real_cmd_dir="${cmd_dir}/remote"
	fi
	if [ "${local}" != 'true' ]; then
		if [ -z "${hosts}" ] || [ "${hosts}" == "127.0.0.1" ] || [ "${hosts}" == "localhost" ]; then
			local real_cmd_dir="${cmd_dir}/remote"
		fi
	fi
	if [ "${byhost}" == 'true' ]; then
		local real_cmd_dir="${real_cmd_dir}/byhost"
	fi

	if [ "${local}" != 'true' ] && [ "${cmd}" != 'dry' ]; then
		# TODO: Parallel ping and copy
		echo "${hosts}" | while read host; do
			if [ ! -z "${host}" ] && [ "${host}" != '127.0.0.1' ] && [ "${host}" != 'localhost' ]; then
				ssh_ping "${host}"
			fi
		done
		echo "${hosts}" | while read host; do
			if [ ! -z "${host}" ] && [ "${host}" != '127.0.0.1' ] && [ "${host}" != 'localhost' ]; then
				cp_env_to_host "${integrated}" "${local_cache_env}" "${host}" "${remote_env_parent}"
			fi
		done
	fi

	if [ "${byhost}" != 'true' ]; then
		if [ "${cmd}" == 'run' ] || [ "${cmd}" == 'dry' ]; then
			if [ "${cmd}" == 'dry' ]; then
				local rendered="${ti_file}.sh"
			else
				local base_name=`basename "${ti_file}"`
				local rendered="/tmp/ti_file_rendered.${base_name}.`date +%s`.${RANDOM}.sh"
			fi
			python "${here}/ti_file.py" 'render' "${ti_file}" \
				"${integrated}" "${conf_templ_dir}" "${cache_dir}" "${mod_names}" "${cmd_hosts}" "${indexes}" "${ti_args}" > "${rendered}"
			chmod +x "${rendered}"
			if [ "${cmd}" == "run" ]; then
				bash "${rendered}"
				rm -f "${rendered}"
			fi
			return 0
		fi

		local mods=`python "${here}/ti_file.py" 'mods' \
			"${ti_file}" "${integrated}" "${conf_templ_dir}" "${cache_dir}" "${mod_names}" "${cmd_hosts}" "${indexes}" "${ti_args}"`

		local has_func=`func_exists "ti_file_cmd_${cmd}"`

		# TODO: loop without sub-process
		echo "${mods}" | while read mod; do
			if [ -z "${mod}" ]; then
				continue
			fi
			local index=`echo "${mod}" | awk -F '\t' '{print $1}'`
			local name=`echo "${mod}" | awk -F '\t' '{print $2}'`
			local dir=`echo "${mod}" | awk -F '\t' '{print $3}'`
			local conf=`echo "${mod}" | awk -F '\t' '{print $4}'`
			local host=`echo "${mod}" | awk -F '\t' '{print $5}'`

			if [ -z "${host}" ] || [ "${host}" == '127.0.0.1' ] || [ "${host}" == 'localhost' ] || [ "${local}" == 'true' ]; then
				local has_script=`test -f "${real_cmd_dir}/${cmd}.sh" && echo true`
				if [ "${has_script}" != 'true' ] && [ "${local}" != 'true' ]; then
					local local_has_script=`test -f "${cmd_dir}/local/${cmd}.sh" && echo true`
					if [ "${local_has_script}" == 'true' ]; then
						echo "<auto add '-l': script ${cmd}.sh not found in remote mode, but found in local mode>" >&2
						local local='true'
						local real_cmd_dir="${cmd_dir}/local"
						local has_script='true'
					fi
				fi
			else
				local has_script=`ssh_exe "${host}" "test -f \"${real_cmd_dir}/${cmd}.sh\" && echo true"`
			fi

			if [ "${has_script}" == 'true' ]; then
				if [ -z "${host}" ] || [ "${host}" == '127.0.0.1' ] || [ "${host}" == 'localhost' ] || [ "${local}" == 'true' ]; then
					if [ -z "${host}" ]; then
						local host=`must_print_ip`
					fi
					local dir=`abs_path "${dir}"`
					if [ -z "${cmd_args+x}" ]; then
						bash "${real_cmd_dir}/${cmd}.sh" "${index}" "${name}" "${dir}" "${conf}" "${host}"
					else
						bash "${real_cmd_dir}/${cmd}.sh" "${index}" "${name}" "${dir}" "${conf}" "${host}" "${cmd_args[@]}"
					fi
				else
					if [ -z "${cmd_args+x}" ]; then
						call_remote_func "${host}" "${remote_env}" script_exe "${real_cmd_dir}/${cmd}.sh" \
							"${index}" "${name}" "${dir}" "${conf}" "${host}"
					else
						call_remote_func "${host}" "${remote_env}" script_exe "${real_cmd_dir}/${cmd}.sh" \
							"${index}" "${name}" "${dir}" "${conf}" "${host}" "${cmd_args[@]}"
					fi
				fi
				continue
			fi
			if [ "${has_func}" == 'true' ]; then
				if [ -z "${host}" ] || [ "${host}" == '127.0.0.1' ] || [ "${host}" == 'localhost' ]; then
					if [ -z "${cmd_args+x}" ]; then
						"ti_file_cmd_${cmd}" "${index}" "${name}" "${dir}" "${conf}" "${host}"
					else
						"ti_file_cmd_${cmd}" "${index}" "${name}" "${dir}" "${conf}" "${host}" "${cmd_args[@]}"
					fi
				else
					if [ -z "${cmd_args+x}" ]; then
						call_remote_func "${host}" "${remote_env}" "ti_file_cmd_${cmd}" "${index}" "${name}" \
							"${dir}" "${conf}" "${host}"
					else
						call_remote_func "${host}" "${remote_env}" "ti_file_cmd_${cmd}" "${index}" "${name}" \
							"${dir}" "${conf}" "${host}" "${cmd_args[@]}"
					fi
				fi
				continue
			fi
			if [ ! -f "${real_cmd_dir}/${cmd}.sh.summary" ]; then
				if [ -z "${host}" ] || [ "${host}" == '127.0.0.1' ] || [ "${host}" == 'localhost' ]; then
					echo "script not found: ${real_cmd_dir}/${cmd}.sh" >&2
				else
					echo "script not found: ${host}:${real_cmd_dir}/${cmd}.sh" >&2
				fi
				return 1
			fi
		done
		# summary always run on local mode
		local summary="${real_cmd_dir}/${cmd}.sh.summary"
		if [ -f "${summary}" ]; then
			if [ -z "${cmd_args+x}" ]; then
				bash "${summary}" "${mods}"
			else
				bash "${summary}" "${mods}" "${cmd_args[@]}"
			fi
		fi
	else
		echo "${hosts}" | while read host; do
			if [ -z "${host}" ]; then
				local host='127.0.0.1'
			fi
			if [ "${local}" == 'true' ]; then
				local has_script=`test -f "${real_cmd_dir}/${cmd}.sh" && echo true`
				if [ "${has_script}" == 'true' ]; then
					if [ -z "${cmd_args+x}" ]; then
						bash "${real_cmd_dir}/${cmd}.sh" "${host}"
					else
						bash "${real_cmd_dir}/${cmd}.sh" "${host}" "${cmd_args[@]}"
					fi
				elif [ ! -f "${real_cmd_dir}/${cmd}.sh.summary" ]; then
					echo "script not found: ${real_cmd_dir}/${cmd}.sh" >&2
					return 1
				fi
			else
				local has_script=`ssh_exe "${host}" "test -f \"${real_cmd_dir}/${cmd}.sh\" && echo true"`
				if [ "${has_script}" == 'true' ]; then
					if [ -z "${cmd_args+x}" ]; then
						call_remote_func "${host}" "${remote_env}" script_exe "${real_cmd_dir}/${cmd}.sh" "${host}"
					else
						call_remote_func "${host}" "${remote_env}" script_exe "${real_cmd_dir}/${cmd}.sh" \
							"${host}" "${cmd_args[@]}"
					fi
				elif [ ! -f "${real_cmd_dir}/${cmd}.sh.summary" ]; then
					echo "script not found: ${host}:${real_cmd_dir}/${cmd}.sh" >&2
					return 1
				fi
			fi
		done
		# summary always run on local mode
		local summary="${real_cmd_dir}/${cmd}.sh.summary"
		if [ -f "${summary}" ]; then
			if [ -z "${cmd_args+x}" ]; then
				bash "${summary}" "${hosts}"
			else
				bash "${summary}" "${hosts}" "${cmd_args[@]}"
			fi
		fi
	fi
}
export -f ti_file_exe

function split_ti_args()
{
	if [ -z "${1+x}" ]; then
		echo "[func split_ti_args] usage: <func> args" >&2
		return 1
	fi

	local args_str="${1}"
	local args=(${args_str//#/ })
	for arg in "${args[@]}"; do
		echo "${arg}"
	done
}
export -f split_ti_args
