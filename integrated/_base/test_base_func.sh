#!/bin/bash

source "${integrated}/_base/cmd_ti.sh"

function get_mod_ver()
{
	local mod="${1}"
	local ti_file="${2}"
	if [ -z "${mod}" ] || [ -z "${ti_file}" ]; then
		echo "[func get_mod_ver] mod ti_file [ti_file_args]"
		return 1
	fi

	if [ -z "${3+x}" ]; then
		local ti_file_args=""
	else
		local ti_file_args="${3}"
	fi
	local ver=`cmd_ti -k "${args}" -m "${mod}" "${ti_file}" ver ver | awk '{print "mod:"$1",ver:"$2}'`
	local failed=`echo "${ver}" | grep 'unknown'`
	if [ ! -z "${failed}" ]; then
		return 1
	fi
	local git=`cmd_ti -k "${args}" -m "${mod}" "${ti_file}" ver githash | awk '{print "git:"$2}'`
	echo "${ver},${git}"
}
export -f get_mod_ver

function get_ti_cluster_mysql_host() {
	local ti_file="${1}"
	if [ -z "${ti_file}" ]; then
		echo "[func get_ti_cluster_mysql_host] cmd ti_file [index] [ti_file_args]"
		return 1
	fi

	if [ -z "${2+x}" ]; then
		local index="0"
	else
		local index="${2}"
	fi

	if [ -z "${3+x}" ]; then
		local ti_file_args=""
	else
		local ti_file_args="${3}"
	fi

	cmd_ti -i "${index}" -k "${ti_file_args}" "${ti_file}" "mysql_host"
}
export -f get_ti_cluster_mysql_host

function get_ti_cluster_mysql_port() {
	local ti_file="${1}"
	if [ -z "${ti_file}" ]; then
		echo "[func get_ti_cluster_mysql_port] cmd ti_file [index] [ti_file_args]"
	return 1
	fi

	if [ -z "${2+x}" ]; then
		local index="0"
	else
		local index="${2}"
	fi

	if [ -z "${3+x}" ]; then
		local ti_file_args=""
	else
		local ti_file_args="${3}"
	fi

	cmd_ti -i "${index}" -k "${ti_file_args}" "${ti_file}" "mysql_port"
}
export -f get_ti_cluster_mysql_port

function load_tpch_data_to_mysql()
{
	if [ -z "${6+x}" ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ] || [ -z "${5}" ] || [ -z "${6}" ]; then
		echo "[func load_tpch_data_to_mysql] cmd mysql_host mysql_port schema_dir data_dir db table"
		return 1
	fi

	local mysql_host="${1}"
	local mysql_port="${2}"
	local schema_dir="${3}"
	local data_dir="${4}"
	local db="${5}"
	local table="${6}"

	local blocks=`ls "${data_dir}" | grep "${table}" | wc -l`

	local create_table_stmt=`cat "${schema_dir}/${table}.ddl" | tr -s "\n" " "`
	mysql -u root -P "${mysql_port}" -h "${mysql_host}" -e "CREATE DATABASE IF NOT EXISTS ${db}"
	mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" -e "${create_table_stmt}"

	if [ "${blocks}" == "1" ]; then
		local data_file="${data_dir}/${table}.tbl"
		mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" \
			-e "load data local infile '${data_file}' into table ${table} fields terminated by '|' lines terminated by '|\n';"
	else
		for ((i=1; i<${blocks}+1; ++i)); do
			local data_file="${data_dir}/${table}.tbl.${i}"
			mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" \
				-e "load data local infile '${data_file}' into table ${table} fields terminated by '|' lines terminated by '|\n';" &
		done
		wait
	fi
}
export -f load_tpch_data_to_mysql

function load_tpch_data_to_ti_cluster()
{
	if [ -z "${5+x}" ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ] || [ -z "${5}" ]; then
		echo "[func load_tpch_data_to_ti_cluster] cmd ti_file schema_dir data_dir db table [ti_file_args]"
		return 1
	fi

	local ti_file="${1}"
	local schema_dir="${2}"
	local data_dir="${3}"
	local db="${4}"
	local table="${5}"

	if [ -z "${6+x}" ]; then
		local ti_file_args=""
	else
		local ti_file_args="${6}"
	fi

	local mysql_host=`get_ti_cluster_mysql_host "${ti_file}" "0" "${ti_file_args}"`
	local mysql_port=`get_ti_cluster_mysql_port "${ti_file}" "0" "${ti_file_args}"`

	load_tpch_data_to_mysql "${mysql_host}" "${mysql_port}" "${schema_dir}" "${data_dir}" "${db}" "${table}"
}
export -f load_tpch_data_to_ti_cluster
