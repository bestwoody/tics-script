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

	if [ "`is_local_host ${host}`" == 'true' ]; then
		if [ ! -f "${info_file}" ]; then
			return
		fi
		get_value "${info_file}" "${key}"
	else
		local has_info=`ssh_exe "${host}" "test -f \"${info_file}\" && echo true"`
		if [ "${has_info}" != 'true' ]; then
			return
		fi
		ssh_exe "${host}" "grep \"${key}\" \"${info_file}\"" | awk '{print $2}'
	fi
}
export -f ssh_get_value_from_proc_info

function from_mods_random_mod()
{
	if [ -z "${2+x}" ]; then
		echo "[func from_mods_random_mod] usage: <func> mods_info_lines candidate_type [index_only=true]" >&2
		return 1
	fi

	local mods="${1}"
	local type="${2}"

	local index_only='true'
	if [ ! -z "${3+x}" ]; then
		local index_only="${3}"
	fi

	local instances=`echo "${mods}" | { grep $'\t'"${type}" || test $? = 1; }`
	if [ -z "${instances}" ]; then
		return
	fi
	local count=`echo "${instances}" | wc -l | awk '{print $1}'`

	local index="${RANDOM}"
	local index_base_1=$((index % count + 1))
	local line=`echo "${instances}" | head -n "${index_base_1}" | tail -n 1`
	if [ "${index_only}" == 'true' ]; then
		echo "${line}" | awk '{print $1}'
	else
		echo "${line}"
	fi
}
export -f from_mods_random_mod

function from_mods_by_type()
{
	if [ -z "${2+x}" ]; then
		echo "[func from_mods_random_mod] usage: <func> mods_info_lines candidate_type" >&2
		return 1
	fi

	local mods="${1}"
	local type="${2}"
	echo "${mods}" | { grep $'\t'"${type}" || test $? = 1; }
}
export -f from_mods_by_type

function from_mod_get_index()
{
	if [ -z "${1+x}" ]; then
		echo "[func from_mod_get_index] usage: <func> mod_info_line" >&2
		return 1
	fi
	local mod="${1}"
	echo "${mod}" | awk -F '\t' '{print $1}'
}
export -f from_mod_get_index

function from_mod_get_name()
{
	if [ -z "${1+x}" ]; then
		echo "[func from_mod_get_name] usage: <func> mod_info_line" >&2
		return 1
	fi
	local mod="${1}"
	echo "${mod}" | awk -F '\t' '{print $2}'
}
export -f from_mod_get_name

function from_mod_get_dir()
{
	if [ -z "${1+x}" ]; then
		echo "[func from_mod_get_dir] usage: <func> mod_info_line" >&2
		return 1
	fi
	local mod="${1}"
	echo "${mod}" | awk -F '\t' '{print $3}'
}
export -f from_mod_get_dir

function from_mod_get_conf()
{
	if [ -z "${1+x}" ]; then
		echo "[func from_mod_get_conf] usage: <func> mod_info_line" >&2
		return 1
	fi
	local mod="${1}"
	echo "${mod}" | awk -F '\t' '{print $4}'
}
export -f from_mod_get_conf

function from_mod_get_host()
{
	if [ -z "${1+x}" ]; then
		echo "[func from_mod_get_host] usage: <func> mod_info_line" >&2
		return 1
	fi
	local mod="${1}"
	local host=`echo "${mod}" | awk -F '\t' '{print $5}'`
	if [ -z "${host}" ]; then
		local host=`must_print_ip`
	fi
	echo "${host}"
}
export -f from_mod_get_host

function from_mod_get_tidb_port()
{
	if [ -z "${1+x}" ]; then
		echo "[func from_mod_get_host] usage: <func> mod_info_line" >&2
		return 1
	fi
	local mod="${1}"
	local dir=`from_mod_get_dir "${mod}"`
	local host=`from_mod_get_host "${mod}"`
	ssh_get_value_from_proc_info "${host}" "${dir}" 'tidb_port'
}
export -f from_mod_get_tidb_port
