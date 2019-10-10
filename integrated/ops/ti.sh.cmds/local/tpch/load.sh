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
	local dbgen_bin_dir="/tmp/ti/master/bins"

	local db=`echo "tpch_${scale}" | tr '.' '_'`

	local port=`ssh_get_value_from_proc_info "${host}" "${dir}" 'tidb_port'`
	if [ -z "${port}" ]; then
		echo "[${host}] ${dir}: getting tidb port failed" >&2
		return 1
	fi

	if [ "${table}" != 'all' ]; then
		local tables=("${table}")
	else
		local tables=(customer nation orders part region supplier partsupp lineitem)
	fi

	local file="${integrated}/conf/tools.kv"
	local dbgen_url=`cross_platform_get_value "${file}" "dbgen_url"`
	local dists_dss_url=`cross_platform_get_value "${file}" "dists_dss_url"`

	for table in ${tables[@]}; do
		local table_dir="${data_dir}/tpch_s`echo ${scale} | tr '.' '_'`_b${blocks}/${table}"
		echo "=> [$host] loading ${table}"
		generate_tpch_data "${dbgen_url}" "${dbgen_bin_dir}" "${table_dir}" "${scale}" "${table}" "${blocks}" "${dists_dss_url}"
		echo '   generated'
		load_tpch_data_to_mysql "${host}" "${port}" "${schema_dir}" "${table_dir}" "${db}" "${table}"
		echo "   loaded"
	done
}

ti_cmd_tpch_load "${@}"