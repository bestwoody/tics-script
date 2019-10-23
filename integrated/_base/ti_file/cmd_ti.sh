#!/bin/bash

function ti_file_cmd_list()
{
	local dir="${1}"
	if [ ! -z "${2+x}" ] && [ ! -z "${2}" ]; then
		local parent="${2}/"
	else
		local parent=''
	fi

	ls "${dir}" | while read f; do
		if [ "${f:0:1}" == '_' ]; then
			continue
		fi
		if [ "${f}" == 'echo.sh' ] || [ "${f}" == 'cmds.sh.summary' ]; then
			continue
		fi
		local has_sh_ext=`echo "${f}" | { grep "sh$" || test $? = 1; }`
		local has_summary_ext=`echo "${f}" | { grep "summary$" || test $? = 1; }`
		if [ ! -z "${has_sh_ext}" ] || [ ! -z "${has_summary_ext}" ]; then
			if [ -f "${dir}/${f}" ]; then
				local name=`basename "${f}" .summary`
				local name=`basename "${name}" .sh`
				echo "${parent}${name}"
				if [ -f "${dir}/${f}.help" ]; then
					cat "${dir}/${f}.help" | awk '{print "    "$0}'
				fi
				continue
			fi
		fi
		if [ -d "${dir}/${f}" ] && [ "${f}" != 'byhost' ]; then
			echo "${f}/*"
			if [ -f "${dir}/${f}/help" ]; then
				cat "${dir}/${f}/help" | awk '{print "    "$0}'
			fi
		fi
	done
}
export -f ti_file_cmd_list

function ti_file_cmd_default_help()
{
	echo "ti.sh cmds"
	echo "    - list global cmds"
	echo "ti.sh help"
	echo "    - detail usage of selectors"
	echo "ti.sh [selectors] cmds"
	echo "    - list cmds, the cmds list would be different under different selectors"
	echo "ti.sh example"
	echo "    - shows a simple example about how to use this tool"
}
export -f ti_file_cmd_default_help

function ti_file_global_cmd_cmds()
{
	if [ -z "${1+x}" ]; then
		echo "[func ti_file_global_cmd_cmds] usage: <func> cmd_dir" >&2
		return
	fi

	local cmd_dir="${1}"

	if [ -d "${cmd_dir}" ]; then
		echo 'command list:'
		ti_file_cmd_list_all "${cmd_dir}" | awk '{print "    "$0}'
	fi
}
export -f ti_file_global_cmd_cmds

function ti_file_global_cmd_help()
{
	echo 'ops/ti [-c conf_templ_dir] [-s cmd =_dir] [-t cache_dir] [-k ti_file_kvs] [-m pd,tikv,..] [-h host,host] [-i mod_index] [-b] [-l] ti_file_path cmd(run|stop|fstop|status|..) [args]'
	echo '    -c:'
	echo '        specify the config template dir, will be `ops/../conf` if this arg is not provided.'
	echo '    -s:'
	echo '        specify the sub-command dir, will be `ops/ti.sh.cmds/remote` if this arg is not provided.'
	echo '    -t:'
	echo '        specify the cache dir for download bins and other things in all hosts.'
	echo '        will be `/tmp/ti` if this arg is not provided.'
	echo '    -k:'
	echo '        specify the key-value(s) string, will be used as vars in the .ti file, format: k=v#k=v#..'
	echo '    -m:'
	echo '        the module name, could be one of pd|tikv|tidb|tiflash|rngine.'
	echo '        and could be multi modules like: `pd,tikv`.'
	echo '        if this arg is not provided, it means all modules.'
	echo '    -h:'
	echo '        the host names, format: `host,host,..`'
	echo '        if this arg is not provided, it means all specified host names in the .ti file.'
	echo '    -i:'
	echo '        specify the module index. eg, 3 tikvs in a cluster, then we have tikv[0], [1], [2].'
	echo '        if this arg is not provided, it means all.'
	echo '    -b:'
	echo '        execute command on each host(node).'
	echo '        if this arg is not provided, execute command on each module.'
	echo '    -l:'
	echo '        execute command on local(of master) mode instead of ssh executing.'
	echo '        use `ops/local/ti.sh.cmds` as command dir instead of `ops/remote/ti.sh.cmds`'
	echo '    cmd:'
	echo '        could be one of run|stop|fstop|status.'
	echo '        (`up` and `down` are aliases of `run` and `stop`)'
	echo '        and could be one of `ops/local|remote/ti.sh.cmds/<command>.sh`'
	echo '        (could be one of `ops/ti.sh.cmds/local|remote/byhost/<command>.sh` if `-b`)'
	echo '    args:'
	echo '        the args pass to the cmd script.'
}
export -f ti_file_global_cmd_help

function ti_file_exe_global_cmd()
{
	if [ -z "${2+x}" ]; then
		echo "[func ti_file_exe_global_cmd] usage: <func> cmd cmd_dir" >&2
		return
	fi

	local cmd="${1}"
	local cmd_dir="${2}"
	shift 2

	if [ "${cmd}" == 'cmds' ]; then
		ti_file_cmd_list "${cmd_dir}/global"
		return
	fi

	local has_func=`func_exists "ti_file_global_cmd_${cmd}"`
	if [ "${has_func}" == 'true' ]; then
		if [ -z "${1+x}" ]; then
			"ti_file_global_cmd_${cmd}"
		else
			local cmd_args=("${@}")
			"ti_file_global_cmd_${cmd}" "${cmd_args[@]}"
		fi
		return
	fi

	if [ -f "${cmd_dir}/global/${cmd}.sh" ]; then
		if [ -z "${1+x}" ]; then
			bash "${cmd_dir}/global/${cmd}.sh"
		else
			local cmd_args=("${@}")
			bash "${cmd_dir}/global/${cmd}.sh" "${cmd_args[@]}"
		fi
		return
	fi

	echo "error: unknown cmd '${cmd}', usage: "
	ti_file_cmd_default_help | awk '{print "    "$0}'
	return 1
}
export -f ti_file_exe_global_cmd

function cmd_ti()
{
	if [ -z "${1+x}" ]; then
		ti_file_cmd_default_help
		return
	fi

	local conf_templ_dir="${integrated}/conf"
	local cmd_dir="${integrated}/ops/ti.sh.cmds"
	local cache_dir="/tmp/ti"
	local mods=""
	local hosts=""
	local ti_args=""
	local byhost="false"
	local local="false"
	local indexes=""

	while getopts ':k:c:m:s:h:i:t:bl' OPT; do
		case ${OPT} in
			c)
				local conf_templ_dir="${OPTARG}";;
			s)
				local cmd_dir="${OPTARG}";;
			t)
				local cache_dir="${OPTARG}";;
			m)
				local mods="${OPTARG}";;
			h)
				local hosts="${OPTARG}";;
			i)
				local indexes="${OPTARG}";;
			k)
				local ti_args="${OPTARG}";;
			b)
				local byhost="true";;
			l)
				local local="true";;
			?)
				echo '[func cmd_ti] illegal option(s)' >&2
				echo '' >&2
				ti_file_cmd_default_help >&2
				return 1;;
		esac
	done
	shift $((${OPTIND} - 1))

	local ext=`print_file_ext "${1}"`
	if [ "${ext}" != 'ti' ]; then
		cmd="${1}"
		shift 1
	else
		local ti_file="${1}"
		local cmd="${2}"
		shift 2
	fi

	local cmd_args=("${@}")

	if [ -z "${ti_file}" ] && [ ! -z "${cmd}" ]; then
		if [ -z "${cmd_args+x}" ]; then
			ti_file_exe_global_cmd "${cmd}" "${cmd_dir}"
		else
			ti_file_exe_global_cmd "${cmd}" "${cmd_dir}" "${cmd_args[@]}"
		fi
		return
	fi

	if [ -z "${ti_file}" ]; then
		ti_file_cmd_default_help "${cmd_dir}" >&2
		return 1
	fi

	if [ -z "${cmd}" ]; then
		local cmd="status"
	fi
	if [ "${cmd}" == "up" ]; then
		local cmd="run"
	fi
	if [ "${cmd}" == "down" ]; then
		local cmd="stop"
	fi

	auto_error_handle
	if [ -z "${cmd_args+x}" ]; then
		ti_file_exe "${cmd}" "${ti_file}" "${conf_templ_dir}" "${cmd_dir}" "${ti_args}" "${mods}" "${hosts}" "${indexes}" "${byhost}" "${local}" "${cache_dir}"
	else
		ti_file_exe "${cmd}" "${ti_file}" "${conf_templ_dir}" "${cmd_dir}" "${ti_args}" "${mods}" "${hosts}" "${indexes}" "${byhost}" "${local}" "${cache_dir}" "${cmd_args[@]}"
	fi
}
export -f cmd_ti
