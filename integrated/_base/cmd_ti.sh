#!/bin/bash

function help_cmd_ti()
{
	echo 'ops/ti.sh [-c conf_templ_dir] [-a ti_file_kvs] [-m pd|tikv|...] ti_file_path mod_command(run|stop|...)'
	echo 'ops/ti.sh [-c conf_templ_dir] [-a ti_file_kvs] -h ti_file_path host_command(uptime|free|...)'
	echo '    -c:'
	echo '        Specify the config template dir, will be `ops/../conf` if this arg is not provided'
	echo '    -a:'
	echo '        Specify the key-value(s) string, will be used as vars in the .ti file, format: k=v#k=v#...'
	echo '    -m:'
	echo '        The module name, could be one of pd|tikv|tidb|tiflash|rngine.'
	echo '        And could be multi modules like: `pd,tikv`.'
	echo '        If this arg is not provided, it means all modules.'
	echo '    -h:'
	echo '        The host names, format: `host,host`'
	echo '        If this arg is not provided, it means all specified host names in the .ti file.'
	echo '    mod_command:'
	echo '        Execute this command on each module.'
	echo '        Could be one of run|stop|status|prop|fstop.'
	echo '        And could be one of `ops/ti.sh.cmds/mod/<command>.sh`'
	echo '        `up` and `down` are aliases of `run` and `stop`'
	echo '    host_command:'
	echo '        Execute this command on each host.'
	echo '        Could be one of `ops/ti.sh.cmds/host/<command>.sh`'
}
export -f help_cmd_ti

function cmd_ti()
{
	local conf_templ_dir=""
	local mods=""
	local hosts=""
	local ti_args=""
	local cmd_dir="${integrated}/ops/ti.sh.cmds"

	while getopts ':a:c:m:s:h:' OPT; do
		case ${OPT} in
			a)
				local ti_args="${OPTARG}";;
			c)
				local conf_templ_dir="${OPTARG}";;
			m)
				local mods="${OPTARG}";;
			h)
				local hosts="${OPTARG}";;
			s)
				local cmd_dir="${OPTARG}";;
			?)
				echo '[func cmd_ti] illegal option(s)' >&2
				echo '' >&2
				help_cmd_ti >&2
				return 1;;
		esac
	done
	shift $((${OPTIND} - 1))

	local ti_file="${1}"
	local cmd="${2}"
	shift 2

	local cmd_args=("${@}")

	if [ -z "${ti_file}" ]; then
		help_cmd_ti >&2
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

	if [ -z "${conf_templ_dir}" ]; then
		local conf_templ_dir="${integrated}/conf"
	fi

	auto_error_handle
	ti_file_exe "${cmd}" "${ti_file}" "${conf_templ_dir}" "${ti_args}" "${mods}" "${hosts}" "${cmd_args[@]}"
}
export -f cmd_ti
