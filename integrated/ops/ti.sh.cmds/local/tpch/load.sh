#!/bin/bash

function ti_cmd_tpch_load
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ "${mod_name}" != 'tidb' ]; then
		return
	fi

	if [ -z "${2+x}" ]; then
		echo "[cmd tpch/load.sh] usage: <cmd> scale table(all|lineitem|...) [data_dir={integrated}/data/tpch] [blocks=4]" >&2
		return
	fi

	local scale="${1}"
	local table="${2}"

	if [ ! -z "${3+x}" ] && [ ! -z "${3}" ]; then
		local data_dir="${3}"
	else
		local data_dir="${integrated}/data/tpch"
	fi

	if [ ! -z "${3+x}" ] && [ ! -z "${3}" ]; then
		local blocks="${4}"
	else
		local blocks='4'
	fi

	local schema_dir="${integrated}/resource/tpch/mysql/schema"
	local db=`echo "tpch_${scale}" | tr '.' '_'`

	local info_file="${dir}/proc.info"
	local has_info=`ssh_exe "${host}" "test -f \"${info_file}\" && echo true"`
	if [ "${has_info}" != 'true' ]; then
		echo "[${host}] ${dir}/proc.info missed, skipped" >&2
		return
	fi

	local port=`ssh_exe "${host}" "grep tidb_port \"${info_file}\"" | awk '{print $2}'`
	if [ -z "${port}" ]; then
		echo "[${host}] ${dir}: getting tidb port failed" >&2
		return 1
	fi

	if [ "${table}" == 'all' ]; then
		local tables=(customer nation orders part region supplier partsupp lineitem)
		for table in ${tables[@]}; do
			local table_dir="${data_dir}/tpch_s`echo ${scale} | tr '.' '_'`_b${blocks}/${table}"
			echo "=> loading ${table}"
			load_tpch_data_to_mysql "${host}" "${port}" "${schema_dir}" "${table_dir}" "${db}" "${table}"
			echo "   done"
		done
	else
		local table_dir="${data_dir}/tpch_s`echo ${scale} | tr '.' '_'`_b${blocks}/${table}"
		load_tpch_data_to_mysql "${host}" "${port}" "${schema_dir}" "${table_dir}" "${db}" "${table}"
		echo 'done'
	fi
}

ti_cmd_tpch_load "${@}"
