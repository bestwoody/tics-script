#!/bin/bash

function load_tpch_data()
{
	local tidb_addr="${1}"

	if [ -z "${tidb_addr}" ]; then
	    echo "[func load_tpch_data] cmd tidb_addr [db] [table] [blocks] [schema_dir] [data_dir]"
	    return 1
	fi

	local tidb_host=`awk -F ":" '{print $1}'`
	local tidb_port=`awk -F ":" '{print $2}'`

	if [ -z "${2+x}" ]; then
		local db='default'
	else
		local db="${2}"
	fi

	if [ -z "${3+x}" ]; then
		local table='lineitem'
	else
		local table="${3}"
	fi

	if [ -z "${4+x}" ]; then
		local blocks='16'
	else
		local blocks="${4}"
	fi

	if [ -z "${5+x}" ]; then
		local schema_dir="/data1/data_loads/tpch/schema"
	else
		local schema_dir="${5}"
	fi

	if [ -z "${6+x}" ]; then
		local data_dir='/data1/data_loads/tpch/data'
	else
		local data_dir="${6}"
	fi

	mkdir -p "./tmp/schema"
	local file="./tmp/schema/${table}.ddl.${db}"

	echo "CREATE DATABASE IF NOT EXISTS ${db};" > "$file"
	echo "USE ${db};" >> "$file"
	echo "" >> "$file"
	cat ./${schema_dir}/${table}.ddl >> "$file"

	local port=`get_value "${dir}/proc.info" 'tidb_port'`
	mysql -h "${tidb_host}" -P "${tidb_port}" -u root < "$file"

	if [ "${blocks}" == "1" ]; then
		file="${data_dir}/${db}/${table}.tbl"
		mysql -h "${host}" -P "${port}" -u root -D "${db}" \
			-e "load data local infile '${file}' into table ${table} fields terminated by '|' lines terminated by '|\n';"
	else
		for ((i=1; i<${blocks}+1; ++i)); do
			file="${data_dir}/${db}_${blocks}/${table}.tbl.${i}"
			mysql -h "${host}" -P "${port}" -u root -D "${db}" \
				-e "load data local infile '${file}' into table ${table} fields terminated by '|' lines terminated by '|\n';" &
		done
		wait
	fi
}

load_tpch_data "${@}"
