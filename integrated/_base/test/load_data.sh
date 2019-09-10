#!/bin/bash

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