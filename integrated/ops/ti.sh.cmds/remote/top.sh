#!/bin/bash

function cmd_ti_top()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	local show_title='false'
	if [ ! -z "${1}" ]; then
		local show_title="${1}"
	fi
	if [ "${show_title}" == 'title' ]; then
		local show_title='true'
	fi

	local indent=''
	if [ "${show_title}" == 'true' ]; then
		echo "=> ${mod_name} #${index} ${dir}"
		local indent='    '
	fi

	local info_file="${dir}/proc.info"
	if [ ! -f "${info_file}" ]; then
		echo "${indent}missed"
		return
	fi

	local pid=`get_value "${dir}/proc.info" 'pid'`
	if [ -z "${pid}" ]; then
		echo "${indent}not running"
		return
	fi

	if [ "`uname`" == "Darwin" ]; then
		local result=`top -pid "${pid}" -l 1 | { grep "${pid}" || test $? = 1; }`
	else
		local result=`top -p "${pid}" -bn 1 | { grep "${pid}" || test $? = 1; }`
	fi

	if [ -z "${result}" ]; then
		echo "${indent}not running"
	else
		echo "${indent}${result}"
	fi
}

cmd_ti_top "${@}"
