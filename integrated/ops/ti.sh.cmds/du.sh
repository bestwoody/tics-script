#!/bin/bash

function cmd_ti_du()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"

	if [ "${mod_name}" == 'pd' ] || [ "${mod_name}" == 'tikv' ]; then
		local data_dir="${dir}/data"
	elif [ "${mod_name}" == 'tiflash' ]; then
		local data_dir="${dir}/db"
	fi

	if [ ! -z "${data_dir+x}" ] && [ -d "${data_dir}" ]; then
		local data=`du -sh "${data_dir}" | awk '{print $1}'`
		local data="${data}/"
	else
		local data=''
	fi

	if [ ! -d "${dir}" ]; then
		local total='MISSED'
	else
		local total=`du -sh "${dir}" | awk '{print $1}'`
	fi

	echo "${mod_name} #${index} (${dir}) ${data}${total}"
}

cmd_ti_du "${@}"
