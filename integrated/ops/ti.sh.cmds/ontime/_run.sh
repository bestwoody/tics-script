#!/bin/bash

function cmd_ontime_run_query()
{
	# ops_call should be "mysql" "mysql/learner" "mysql/learner_by_hint" "mysql/tikv" "ch"
	local ops_call="${1}"
	local queries_dir="${2}"

	local ti_file="${3}"
	local ti_args="${4}"
	local cmd_mod_names="${5}"
	local cmd_hosts="${6}"
	local cmd_indexes="${7}"
	local mods="${8}"

	shift 8

	local query=${1:-'all'}
	local db=${2:-''}
	local tail=${3:-'20'}

	if [ "${query}" == all ]; then
		local query="0,1,2,3,4,5,6,7,8,9,10"
	fi
	if [ ! -z "`echo ${query} | grep ','`" ]; then
		local queries=`echo "${query}" | awk -F ',' '{for ( i=1; i<=NF; i++ ) print $i}'`
		echo "${queries}" | while read query; do
			cmd_ontime_run_query "${ops_call}" "${queries_dir}" \
				"${ti_file}" "${ti_args}" "${cmd_mod_names}" \
				"${cmd_hosts}" "${cmd_indexes}" "${mods}" "${query}" "${db}" "${tail}"
		done
		return
	fi

	local tidb=`from_mods_random_mod "${mods}" 'tidb'`
	if [ -z "${tidb}" ]; then
		echo "[cmd ontime/run] tidb not found in cluster" >&2
		return 1
	fi

	local query_file="${queries_dir}/ontime_q${query}.sql"
	echo "=> ${query_file}, db=${db}"
	"${integrated}/ops/ti.sh"  -h "${cmd_hosts}" -m 'tidb' -i "${tidb}" -k "${ti_args}" "${ti_file}" \
		"${ops_call}" "${query_file}" "${db}" true | tail -n "${tail}"
}
export -f cmd_ontime_run_query
