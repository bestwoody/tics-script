#!/bin/bash

function ti_file_cmd_list()
{
	local dir="${1}"

	if [ ! -z "${2+x}" ] && [ ! -z "${2}" ]; then
		local matching="${2}"
	else
		local matching=''
	fi

	if [ ! -z "${3+x}" ] && [ ! -z "${3}" ]; then
		local parent="${3}/"
	else
		local parent=''
	fi

	ls "${dir}" | while read f; do
		if [ "${f:0:1}" == '_' ] || [ "${f}" == 'help' ]; then
			continue
		fi
		if [ ! -z "${matching}" ]; then
			local matched=`echo "${f}" | grep "${matching}"`
			if [ -z "${matched}" ]; then
				continue
			fi
		fi
		local has_help_ext=`echo "${f}" | { grep "help$" || test $? = 1; }`
		if [ ! -z "${has_help_ext}" ] && [ -f "${dir}/${f}" ]; then
			local name=`basename "${f}" .help`
			local name=`basename "${name}" .summary`
			local name=`basename "${name}" .sh`
			echo "${parent}${name}"
			cat "${dir}/${f}" | awk '{print "    "$0}'
			continue
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
	echo "    - list global cmds [cmd-matching-string]"
	echo "ti.sh help"
	echo "    - detail usage of flags"
	echo "ti.sh [flags] my.ti cmds [cmd-matching-string]"
	echo "    - list cmds, the cmds list would be different under different flags"
	echo "    - eg: ti.sh -l my.ti cmds"
	echo "    - eg: ti.sh -h my.ti cmds"
	echo "ti.sh example"
	echo "    - shows a simple example about how to use this tool"
}
export -f ti_file_cmd_default_help

# TODO: remove
function ti_file_global_cmd_cmds()
{
	if [ -z "${1+x}" ]; then
		echo "[func ti_file_global_cmd_cmds] usage: <func> cmd_dir [matching]" >&2
		return
	fi

	local cmd_dir="${1}"
	if [ ! -z "${2+x}" ] && [ ! -z "${2}" ]; then
		local matching="${2}"
	else
		local matching=''
	fi

	if [ -d "${cmd_dir}" ]; then
		echo 'command list:'
		ti_file_cmd_list_all "${matching}" "${cmd_dir}" | awk '{print "    "$0}'
	fi
}
export -f ti_file_global_cmd_cmds

function ti_file_global_cmd_help()
{
	echo 'usage: ops/ti.sh [mods-selector] [run-mode] ti_file_path cmd [args]'
	echo
	echo 'example: ops/ti.sh my.ti run'
	echo '    cmd:'
	echo '        could be one of run|stop|fstop|status|burn|...'
	echo '        (`up` and `down` are aliases of `run` and `stop`)'
	echo '        and could be one of `{integrated}/ops/ti.sh.cmds/local|remote/<command>.sh`'
	echo '        (could be one of `{integrated}/ops/ti.sh.cmds/local|remote/byhost/<command>.sh` if `-b`)'
	echo '    args:'
	echo '        the args pass to the command script.'
	echo '        format `cmd1:cmd2:cmd3` can be used to execute commands sequently, if all args are empty.'
	echo
	echo 'the selecting flags below are for selecting mods from the .ti file defined a cluster.'
	echo 'example: ops/ti.sh -m pd -i 0,2 my.ti stop'
	echo '    -m:'
	echo '        the module name, could be one of pd|tikv|tidb|tiflash|rngine|sparkm|sparkw.'
	echo '        and could be multi modules like: `pd,tikv`.'
	echo '        if this arg is not provided, it means all modules.'
	echo '    -h:'
	echo '        the host names, format: `host,host,..`'
	echo '        if this arg is not provided, it means all specified host names in the .ti file.'
	echo '    -i:'
	echo '        specify the module index, format: `1,4,3`.'
	echo '        eg, 3 tikvs in a cluster, then we have tikv[0], tikv[1], tikv[2].'
	echo '        if this arg is not provided, it means all.'
	echo
	echo 'the mode flags below decide where and how to execute the command.'
	echo 'example: ops/ti.sh -b -l my.ti top'
	echo '    -b:'
	echo '        execute command on each host(node).'
	echo '        if this arg is not provided, execute command on each module.'
	echo '    -l:'
	echo '        execute command on local(of master) mode instead of ssh executing.'
	echo '        use `{integrated}/ops/local/ti.sh.cmds` as command dir instead of `{integrated}/ops/remote/ti.sh.cmds`'
	echo
	echo 'the configuring flags below are rarely used.'
	echo 'example: ops/ti.sh -c /data/my_templ_dir -s /data/my_cmd_dir -t /tmp/my_cache_dir -k foo=bar my.ti status'
	echo '    -k:'
	echo '        specify the key-value(s) string, will be used as vars in the .ti file, format: k=v#k=v#..'
	echo '    -c:'
	echo '        specify the config template dir, will be `{integrated}/conf` if this arg is not provided.'
	echo '    -s:'
	echo '        specify the sub-command dir, will be `{integrated}/ops/ti.sh.cmds/remote` if this arg is not provided.'
	echo '    -t:'
	echo '        specify the cache dir for download bins and other things in all hosts.'
	echo '        will be `/tmp/ti` if this arg is not provided.'
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
		if [ -z "${1+x}" ]; then
			ti_file_cmd_list "${cmd_dir}/global"
		else
			local cmd_args=("${@}")
			ti_file_cmd_list "${cmd_dir}/global" "${cmd_args[@]}"
		fi
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
		local ti_file=''
		local cmd="${1}"
		shift 1
	else
		local ti_file="${1}"
		local cmd="${2}"
		shift 2
	fi

	local cmd_args=("${@}")
	auto_error_handle

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

	if [ -z "${cmd_args+x}" ]; then
		if [ ! -z "`echo ${cmd} | grep ':'`" ]; then
			local cmds=`echo "${cmd}" | awk -F ':' '{for ( i=1; i<=NF; i++ ) print $i}'`
			echo "${cmds}" | while read cmd; do
				echo "------------"
				echo "${cmd}"
				echo "------------"
				local cmd_and_args=(${cmd})
				if [ "${#cmd_and_args[@]}" == '1' ]; then
					ti_file_exe "${cmd}" "${ti_file}" "${conf_templ_dir}" "${cmd_dir}" "${ti_args}" "${mods}" "${hosts}" "${indexes}" "${byhost}" "${local}" "${cache_dir}"
				else
					local cmd="${cmd_and_args[0]}"
					local cmd_args=${cmd_and_args[@]:1}
					ti_file_exe "${cmd}" "${ti_file}" "${conf_templ_dir}" "${cmd_dir}" "${ti_args}" "${mods}" "${hosts}" "${indexes}" "${byhost}" "${local}" "${cache_dir}" "${cmd_args[@]}"
				fi
			done
		else
			ti_file_exe "${cmd}" "${ti_file}" "${conf_templ_dir}" "${cmd_dir}" "${ti_args}" "${mods}" "${hosts}" "${indexes}" "${byhost}" "${local}" "${cache_dir}"
		fi
	else
		ti_file_exe "${cmd}" "${ti_file}" "${conf_templ_dir}" "${cmd_dir}" "${ti_args}" "${mods}" "${hosts}" "${indexes}" "${byhost}" "${local}" "${cache_dir}" "${cmd_args[@]}"
	fi
}
export -f cmd_ti
