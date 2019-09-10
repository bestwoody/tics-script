#!/bin/bash

function _ti_file_cmd_list()
{
	local dir="${1}"
	ls "${dir}" | grep "sh$" | while read f; do
		echo "    `basename "${f}" .sh`";
	done
}
export -f _ti_file_cmd_list

function ti_file_cmd_list()
{
	local cmd_dir="${1}"
	echo "remote, by module: (default)"
	_ti_file_cmd_list "${cmd_dir}/remote"
	echo "remote, by host: (-b)"
	_ti_file_cmd_list "${cmd_dir}/remote/byhost"
	echo "local, by module: (-l)"
	_ti_file_cmd_list "${cmd_dir}/local"
	echo "local, by host: (-l -b)"
	_ti_file_cmd_list "${cmd_dir}/local/byhost"
}
export -f ti_file_cmd_list

function ti_file_cmd_help()
{
	local cmd_dir="${1}"

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

	echo
	echo 'command list:'

	if [ -d "${cmd_dir}" ]; then
		ti_file_cmd_list "${cmd_dir}" | awk '{print "    "$0}'
	fi
}
export -f ti_file_cmd_help

function cmd_ti()
{
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
				ti_file_cmd_help "${cmd_dir}" >&2
				return 1;;
		esac
	done
	shift $((${OPTIND} - 1))

	local ti_file="${1}"
	local cmd="${2}"
	shift 2

	local cmd_args=("${@}")

	if [ -z "${ti_file}" ]; then
		ti_file_cmd_help "${cmd_dir}" >&2
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