#!/bin/bash

function _db_name_from_scale()
{
	local scale="${1}"
	echo "tpch_${scale}" | tr '.' '_'
}
export -f _db_name_from_scale

function test_cluster_cmd()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_cmd] usage: <func> ports mods ops-ti-args" >&2
		return 1
	fi

	local ports="${1}"
	local mods="${2}"
	shift 2

	if [ -z "${test_cluster_data_dir+x}" ] || [ -z "${test_cluster_data_dir}" ]; then
		echo "[func test_cluster_cmd] var 'test_cluster_data_dir' not defined" >&2
		return 1
	fi

	local ti="${integrated}/ops/ti.sh"
	local ti_file="${integrated}/tests/_base/local_templ.ti"
	local args="ports=+${ports}#dir=${test_cluster_data_dir}/nodes/${ports}"

	"${ti}" -m "${mods}" -k "${args}" "${ti_file}" "${@}"
}
export -f test_cluster_cmd

function test_cluster_ver()
{
	if [ -z "${2+x}" ]; then
		echo "[func test_cluster_ver] usage: <func> ports mod [mod_name_as_prefix=true]" >&2
		return 1
	fi

	local ports="${1}"
	local mod="${2}"
	if [ ! -z "${3+x}" ]; then
		local mod_name_prefix="${3}"
	else
		local mod_name_prefix='true'
	fi

	local ver=`test_cluster_cmd "${ports}" "${mod}" ver ver`
	local failed=`echo "${ver}" | { grep 'unknown' || test $? = 1; }`
	if [ ! -z "${failed}" ]; then
		return 1
	fi

	if [ "${mod_name_prefix}" == 'true' ]; then
		local ver=`echo "${ver}" | awk '{print $1"_ver:"$2}'`
	else
		local ver=`echo "${ver}" | awk '{print "mod:"$1",ver:"$2}'`
	fi

	local githash=`test_cluster_cmd "${ports}" "${mod}" ver githash`
	if [ "${mod_name_prefix}" == 'true' ]; then
		local githash=`echo "${githash}" | awk '{print $1"_git:"$2}'`
	else
		local githash=`echo "${githash}" | awk '{print "git:"$2}'`
	fi
	echo "${ver},${githash}"
}
export -f test_cluster_ver

# TODO: add an arg: mods
function test_cluster_vers()
{
	if [ -z "${1+x}" ]; then
		echo "[func test_cluster_vers] usage: <func> ports" >&2
		return 1
	fi

	local ports="${1}"

	# TODO: can be faster
	local version="`test_cluster_ver "${ports}" "pd"`,`test_cluster_ver "${ports}" "tikv"`,`test_cluster_ver "${ports}" "tidb"`,`test_cluster_ver "${ports}" "tiflash"`,`test_cluster_ver "${ports}" "rngine"`"
	echo "${version}"
}
export -f test_cluster_vers

function test_cluster_prepare()
{
	if [ -z "${2+x}" ]; then
		echo "[func test_cluster_prepare] usage: <func> ports mods" >&2
		return 1
	fi

	local ports="${1}"
	local mods="${2}"
	shift 2

	test_cluster_cmd "${ports}" "${mods}" burn doit
	echo '---'
	test_cluster_cmd "${ports}" "${mods}" run

	local status=`test_cluster_cmd "${ports}" "${mods}" status`
	local status_cnt=`echo "${status}" | wc -l | awk '{print $1}'`
	local ok_cnt=`echo "${status}" | { grep 'OK' || test $? = 1; } | wc -l | awk '{print $1}'`
	if [ "${status_cnt}" != "${ok_cnt}" ]; then
		echo "test cluster prepare failed, status:" >&2
		echo "${status}" >&2
		return 1
	fi
}
export -f test_cluster_prepare

function test_cluster_burn()
{
	if [ -z "${1+x}" ]; then
		echo "[func test_cluster_burn] usage: <func> ports" >&2
		return 1
	fi

	local ports="${1}"
	test_cluster_cmd "${ports}" '' burn doit
}
export -f test_cluster_burn

function _test_cluster_gen_and_load_tpch_table()
{
	if [ -z "${5+x}" ]; then
		echo "[func test_cluster_gen_and_load_tpch_table] usage: <func> ports table scale blocks data_dir" >&2
		return 1
	fi

	local ports="${1}"
	local table="${2}"
	local scale="${3}"
	local blocks="${4}"
	local data_dir="${5}"

	local db=`_db_name_from_scale "${scale}"`
	local schema_dir="${integrated}/resource/tpch/mysql/schema"
	local dbgen_bin_dir="/tmp/ti/master/bins"

	# TODO: remove hard code url
	if [ `uname` == "Darwin" ]; then
		local dbgen_url="http://139.219.11.38:8000/3GdrI/dbgen.tar.gz"
	else
		local dbgen_url="http://139.219.11.38:8000/fCROr/dbgen.tar.gz"
	fi
	local dists_dss_url="http://139.219.11.38:8000/v2TLJ/dists.dss"

	local table_dir="${data_dir}/tpch_s`echo ${scale} | tr '.' '_'`_b${blocks}/${table}"
	generate_tpch_data "${dbgen_url}" "${dbgen_bin_dir}" "${table_dir}" "${scale}" "${table}" "${blocks}" "${dists_dss_url}"

	local mysql_host=`test_cluster_cmd "${ports}" '' 'mysql_host'`
	local mysql_port=`test_cluster_cmd "${ports}" '' 'mysql_port'`

	load_tpch_data_to_mysql "${mysql_host}" "${mysql_port}" "${schema_dir}" "${table_dir}" "${db}" "${table}"
}
export -f _test_cluster_gen_and_load_tpch_table

function test_cluster_load_tpch_table()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_load_tpch_table] usage: <func> ports table scale [blocks] [data_dir]" >&2
		return 1
	fi

	local ports="${1}"
	local table="${2}"
	local scale="${3}"

	if [ ! -z "${4+x}" ] && [ ! -z "${4}" ]; then
		local blocks="${4}"
	else
		local blocks='4'
	fi

	if [ ! -z "${5+x}" ] && [ ! -z "${5}" ]; then
		local data_dir="${5}"
	else
		if [ -z "${test_cluster_data_dir+x}" ] || [ -z "${test_cluster_data_dir}" ]; then
			echo "[func test_cluster_load_tpch_table] var 'test_cluster_data_dir' not defined" >&2
			return 1
		fi
		local data_dir="${test_cluster_data_dir}"
	fi

	local db=`_db_name_from_scale "${scale}"`

	local start_time=`date +%s`
	_test_cluster_gen_and_load_tpch_table "${ports}" "${table}" "${scale}" "${blocks}" "${data_dir}"
	local end_time=`date +%s`
	local elapsed="$((end_time - start_time))"

	echo "${elapsed}	db:${db},table:${table},start_ts:${start_time},end_ts:${end_time}"
}
export -f test_cluster_load_tpch_table

function test_cluster_load_tpch_data()
{
	# TODO: args [blocks] [data_dir]
	if [ -z "${4+x}" ]; then
		echo "[func test_cluster_load_tpch_data] usage: <func> ports scale log_file tags" >&2
		return 1
	fi

	local ports="${1}"
	local scale="${2}"
	local log="${3}"
	local tags="${4}"

	local tables=(customer lineitem nation orders part region supplier partsupp)
	local result=''
	for table in ${tables[@]}; do
		local elapsed=`test_cluster_load_tpch_table "${ports}" "${table}" "${scale}"`
		if [ -z "${elapsed}" ]; then
			echo "[func tpch_perf] failed to load '${table}'" >&2
			return 1
		else
			echo "[func tpch_perf] loaded '${table}'"
		fi
		echo "${elapsed},${tags}" >> ${log}
	done
}
export -f test_cluster_load_tpch_data

function test_cluster_run_tpch()
{
	if [ -z "${4+x}" ]; then
		echo "[func test_cluster_run_tpch] usage: <func> ports scale entry_dir tags"  >&2
		return 1
	fi

	local ports="${1}"
	local scale="${2}"
	local entry_dir="${3}"
	local tags="${4}"

	local db=`_db_name_from_scale "${scale}"`
	local queries_dir="${integrated}/resource/tpch/mysql/queries"

	if [ ! -d "${queries_dir}" ]; then
		echo "[func test_cluster_run_tpch] '${queries_dir}' not a dir" >&2
		return 1
	fi
	for ((i = 1; i < 23; ++i)); do
		if [ ! -f "${queries_dir}/${i}.sql" ]; then
			echo "[func test_cluster_run_tpch] '${queries_dir}/${i}.sql' not exists" >&2
			return 1
		fi
	done

	# TODO: remove this, use 'test_cluster_cmd ... mysql'
	local mysql_host=`test_cluster_cmd "${ports}" '' 'mysql_host'`
	local mysql_port=`test_cluster_cmd "${ports}" '' 'mysql_port'`
	if [ -z "${mysql_host}" ] || [ -z "${mysql_port}" ]; then
		echo "[func test_cluster_run_tpch] get mysql address failed(${mysql_host}:${mysql_port})" >&2
		return 1
	fi

	for ((i = 1; i < 23; ++i)); do
		echo "=> tpch on tidb, scale ${scale}, query #${i}"
		local start_time=`date +%s`
		mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" < "${queries_dir}/${i}.sql" > "${entry_dir}/tidb.q${i}.result"
		local end_time=`date +%s`
		local elapsed="$((end_time - start_time))"
		local elapsed="${elapsed}	cat:tidb,scale:${scale},query:${i},start_ts:${start_time},end_ts:${end_time}"
		# TODO: check result
		echo "${elapsed},${tags}" >> "${entry_dir}/queries.data"
	done
}
export -f test_cluster_run_tpch

function test_cluster_spark_run_tpch()
{
	if [ -z "${4+x}" ]; then
		echo "[func test_cluster_spark_run_tpch] usage: <func> ports scale entry_dir tags"  >&2
		return 1
	fi

	local ports="${1}"
	local scale="${2}"
	local entry_dir="${3}"
	local tags="${4}"

	local db=`_db_name_from_scale "${scale}"`
	local queries_dir="${integrated}/resource/tpch/spark/queries"

	if [ ! -d "${queries_dir}" ]; then
		echo "[func test_cluster_spark_run_tpch] '${queries_dir}' not a dir" >&2
		return 1
	fi
	for ((i = 1; i < 23; ++i)); do
		local sql_file="${queries_dir}/q${i}.sql"
		if [ ! -f "${sql_file}" ]; then
			echo "[func test_cluster_spark_run_tpch] '${sql_file}' not exists" >&2
			return 1
		fi
		local new_file="${entry_dir}/${i}.sql"
		echo "use ${db};" > "${new_file}"
		echo '' >> "${new_file}"
		cat "${sql_file}" >> "${new_file}"
	done

	for ((i = 1; i < 23; ++i)); do
		echo "=> tpch on spark, scale ${scale}, query #${i}"
		local start_time=`date +%s`
		test_cluster_cmd "${ports}" '' beeline -f "${entry_dir}/${i}.sql" > "${entry_dir}/spark.q${i}.result"
		local end_time=`date +%s`
		local elapsed="$((end_time - start_time))"
		local elapsed="${elapsed}	cat:spark,scale:${scale},query:${i},start_ts:${start_time},end_ts:${end_time}"
		# TODO: check result
		echo "${elapsed},${tags}" >> "${entry_dir}/queries.data"
	done
}
export -f test_cluster_spark_run_tpch