#!/bin/bash

function ssh_get_value_from_proc_info()
{
	if [ -z "${3+x}" ]; then
		echo "[ssh_get_value_from_proc_info] host dir key" >&2
		return 1
	fi

	local host="${1}"
	local dir="${2}"
	local key="${3}"

	local info_file="${dir}/proc.info"
	local local_host=`must_print_ip`

	if [ "${host}" == "${local_host}" ]; then
		if [ ! -f "${info_file}" ]; then
			return
		fi
		grep "${key}" "${info_file}" | awk '{print $2}'
	else
		local has_info=`ssh_exe "${host}" "test -f \"${info_file}\" && echo true"`
		if [ "${has_info}" != 'true' ]; then
			return
		fi
		ssh_exe "${host}" "grep \"${key}\" \"${info_file}\"" | awk '{print $2}'
	fi
}
export -f ssh_get_value_from_proc_info
