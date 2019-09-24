#!/bin/bash

function cmd_ti_burn()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"
	local doit="${6}"

	if [ "${dir}" == "/" ]; then
		echo "=> DENIED: rm -f /" >&2
		return 1
	fi

	echo "=> burning: ${dir}"
	if [ "${doit}" == "doit" ]; then
		while true; do
			local up_status=`ti_file_mod_status "${dir}" "${conf_rel_path}"`
			local ok=`echo "${up_status}" | { grep ^OK || test $? = 1; }`
			if [ ! -z "${ok}" ]; then
				ti_file_cmd_fstop "${index}" "${mod_name}" "${dir}" "${conf_rel_path}" 2>&1 | \
					awk '{if ($1 != "=>") print "   "$0}'
			else
			    break
			fi
			sleep 0.5
		done
		if [ ! -d "${dir}" ]; then
			echo "   missed"
		else
			local result=`rm -rf "${dir}" 2>&1`
			if [ ! -z "${result}" ]; then
				echo "   error: ${result}"
				echo "[func cmd_ti_burn] ${result}" >&2
			else
				echo "   burned"
			fi
		fi
	else
		if [ ! -d "${dir}" ]; then
			echo "   MISSED"
		else
			echo "   dry run, append 'doit' to erase data"
		fi
	fi
}

cmd_ti_burn "${@}"
