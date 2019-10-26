#!/bin/bash

function cmd_tpch_run_query()
{
	local queries_dir="${1}"

	local ti_file="${2}"
	local ti_args="${3}"
	local cmd_mod_names="${4}"
	local cmd_hosts="${5}"
	local cmd_indexes="${6}"
	local mods="${7}"

	shift 7

	local query='all'
	if [ ! -z "${1+x}" ]; then
		local query="${1}"
	fi
	local db=''
	if [ ! -z "${2+x}" ]; then
		local db="${2}"
	fi
	local head='5'
	if [ ! -z "${3+x}" ]; then
		local head="${3}"
	fi

	if [ "${query}" == all ]; then
		local query="1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
	fi
	if [ ! -z "`echo ${query} | grep ','`" ]; then
		local queries=`echo "${query}" | awk -F ',' '{for ( i=1; i<=NF; i++ ) print $i}'`
		echo "${queries}" | while read query; do
			cmd_tpch_run_query "${queries_dir}" "${ti_file}" "${ti_args}" "${cmd_mod_names}" \
				"${cmd_hosts}" "${cmd_indexes}" "${mods}" "${query}" "${db}" "${head}"
		done
		return
	fi

	local tidb=`random_mod "${mods}" 'tidb'`
	if [ -z "${tidb}" ]; then
		echo "[cmd tpch/ch] tidb not found in cluster" >&2
		return 1
	fi

	if [ -z "${db}" ] || [ "${db}" == 'auto' ]; then
		local db=`"${integrated}/ops/ti.sh" -h "${cmd_hosts}" -m 'tidb' -i "${tidb}" -k "${ti_args}" "${ti_file}" 'mysql' "show databases" | grep 'tpch'`
		if [ -z "${db}" ]; then
			echo "[cmd tpch/ch] database with 'tpch' prefix not found" >&2
			return 1
		fi
		local db_cnt=`echo "${db}" | wc -l | awk '{print $1}'`
		if [ "${db_cnt}" != '1' ]; then
			echo "[cmd tpch/ch] more than one database with 'tpch' prefix in cluster, can't auto select it" >&2
			return 1
		fi
	fi

	echo "=> ${queries_dir}/${query}.sql, db=${db}"
	local start_time=`date +%s`
	"${integrated}/ops/ti.sh"  -h "${cmd_hosts}" -m 'tidb' -i "${tidb}" -k "${ti_args}" "${ti_file}" \
		'mysql' "${queries_dir}/${query}.sql" "${db}" | head -n "${head}"
	local end_time=`date +%s`
	echo "elapsed: $((end_time - start_time))s"
}
export -f cmd_tpch_run_query
