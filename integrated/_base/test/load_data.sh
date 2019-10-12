#!/bin/bash

function load_tpch_data_to_mysql()
{
	if [ -z "${6+x}" ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ] || [ -z "${5}" ] || [ -z "${6}" ]; then
		echo "[func load_tpch_data_to_mysql] cmd mysql_host mysql_port schema_dir data_dir db table" >&2
		return 1
	fi

	local mysql_host="${1}"
	local mysql_port="${2}"
	local schema_dir="${3}"
	local data_dir="${4}"
	local db="${5}"
	local table="${6}"

	local blocks=`ls "${data_dir}" | { grep "${table}" || test $? = 1; } | wc -l`

	local create_table_stmt=`cat "${schema_dir}/${table}.ddl" | tr -s "\n" " "`
	mysql -u root -P "${mysql_port}" -h "${mysql_host}" -e "CREATE DATABASE IF NOT EXISTS ${db}"
	mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" -e "${create_table_stmt}"

	if [ -f "${data_dir}/${table}.tbl" ] && [ -f "${data_dir}/${table}.tbl.1" ]; then
		echo "[func load_tpch_data_to_mysql] '${data_dir}' not data dir" >&2
		return 1
	fi

	if [ -f "${data_dir}/${table}.tbl" ]; then
		local data_file="${data_dir}/${table}.tbl"
		mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" --local-infile=1 \
			-e "load data local infile '${data_file}' into table ${table} fields terminated by '|' lines terminated by '|\n';"
	else
		for (( i = 1; i < ${blocks} + 1; ++i)); do
			local data_file="${data_dir}/${table}.tbl.${i}"
			if [ ! -f "${data_file}" ]; then
				echo "[func load_tpch_data_to_mysql] ${data_file} not exists" >&2
				return 1
			fi
		done
		for ((i=1; i<${blocks}+1; ++i)); do
			local data_file="${data_dir}/${table}.tbl.${i}"
			mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" --local-infile=1 \
				-e "load data local infile '${data_file}' into table ${table} fields terminated by '|' lines terminated by '|\n';" &
		done
		wait
	fi

	mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" -e "analyze table ${table};"
}
export -f load_tpch_data_to_mysql
