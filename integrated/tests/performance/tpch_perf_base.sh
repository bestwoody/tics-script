#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"

function _test_cluster_gen_and_load_tpch_table()
{
	if [ -z "${5+x}" ]; then
		echo "[func test_cluster_gen_and_load_tpch_table] usage: <func> test_ti_file table scale blocks data_dir" >&2
		return 1
	fi

	local test_ti_file="${1}"
	local table="${2}"
	local scale="${3}"
	local blocks="${4}"
	local data_dir="${5}"

	local db=`_db_name_from_scale "${scale}"`
	local schema_dir="${integrated}/resource/tpch/mysql/schema"
	local dbgen_bin_dir="/tmp/ti/master/bins"

	local file="${integrated}/conf/tools.kv"
	local dbgen_url=`cross_platform_get_value "${file}" "dbgen_url"`
	local dists_dss_url=`cross_platform_get_value "${file}" "dists_dss_url"`

	local table_dir="${data_dir}/tpch_s`echo ${scale} | tr '.' '_'`_b${blocks}/${table}"
	generate_tpch_data "${dbgen_url}" "${dbgen_bin_dir}" "${table_dir}" "${scale}" "${table}" "${blocks}" "${dists_dss_url}"

	local ti="${integrated}/ops/ti.sh"
	local mysql_host=`"${ti}" "${test_ti_file}" 'mysql/host'`
	local mysql_port=`"${ti}" "${test_ti_file}" 'mysql/port'`

	load_tpch_data_to_mysql "${mysql_host}" "${mysql_port}" "${schema_dir}" "${table_dir}" "${db}" "${table}"
}

function test_cluster_load_tpch_table()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_load_tpch_table] usage: <func> test_ti_file table scale [blocks] [data_dir]" >&2
		return 1
	fi

	local test_ti_file="${1}"
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
	_test_cluster_gen_and_load_tpch_table "${test_ti_file}" "${table}" "${scale}" "${blocks}" "${data_dir}"
	local end_time=`date +%s`
	local elapsed="$((end_time - start_time))"

	echo "${elapsed}	db:${db},table:${table},start_ts:${start_time},end_ts:${end_time}"
}

function test_cluster_load_tpch_data()
{
	# TODO: args [blocks] [data_dir]
	if [ -z "${4+x}" ]; then
		echo "[func test_cluster_load_tpch_data] usage: <func> test_ti_file scale log_file tags" >&2
		return 1
	fi

	local test_ti_file="${1}"
	local scale="${2}"
	local log="${3}"
	local tags="${4}"

	local tables=(customer lineitem nation orders part region supplier partsupp)
	local result=''
	for table in ${tables[@]}; do
		local elapsed=`test_cluster_load_tpch_table "${test_ti_file}" "${table}" "${scale}"`
		if [ -z "${elapsed}" ]; then
			echo "[func tpch_perf] failed to load '${table}'" >&2
			return 1
		else
			echo "[func tpch_perf] loaded '${table}'"
		fi
		echo "${elapsed},${tags}" >> ${log}
	done
}

function test_cluster_run_tpch()
{
	if [ -z "${4+x}" ]; then
		echo "[func test_cluster_run_tpch] usage: <func> test_ti_file scale entry_dir tags [query_number=all] [use_tiflash_storage]"  >&2
		return 1
	fi

	local test_ti_file="${1}"
	local scale="${2}"
	local entry_dir="${3}"
	local tags="${4}"

	if [ ! -z "${5+x}" ]; then
		local query_number="${5}"
	else
		local query_number=''
	fi

	if [ ! -z "${6+x}" ]; then
		local use_tiflash_storage="${6}"
	else
		local use_tiflash_storage='false'
	fi


	local db=`_db_name_from_scale "${scale}"`
	if [ "${use_tiflash_storage}" != 'true' ]; then
		local queries_dir="${integrated}/resource/tpch/mysql/queries"
		local cat_tag='tidb_tikv'
	else
		local queries_dir="${integrated}/resource/tpch/mysql/queries_tiflash_hint"
		local cat_tag='tidb_flash'
	fi

	if [ ! -d "${queries_dir}" ]; then
		echo "[func test_cluster_run_tpch] '${queries_dir}' not a dir" >&2
		return 1
	fi

	local ti="${integrated}/ops/ti.sh"
	local mysql_host=`"${ti}" "${test_ti_file}" 'mysql/host'`
	local mysql_port=`"${ti}" "${test_ti_file}" 'mysql/port'`
	if [ -z "${mysql_host}" ] || [ -z "${mysql_port}" ]; then
		echo "[func test_cluster_run_tpch] get mysql address failed(${mysql_host}:${mysql_port})" >&2
		return 1
	fi

	if [ -z "${query_number}" ]; then
		for ((i = 1; i < 23; ++i)); do
			test_cluster_run_tpch "${test_ti_file}" "${scale}" "${entry_dir}" "${tags}" "${i}" "${use_tiflash_storage}"
		done
	else
		echo "=> ${cat_tag}, scale ${scale}, query #${query_number}"
		local sql_file="${queries_dir}/${query_number}.sql"
		if [ ! -f "${sql_file}" ]; then
			echo "[func test_cluster_run_tpch] '${sql_file}' not exists" >&2
			return 1
		fi
		local start_time=`date +%s`
		mysql -u root -P "${mysql_port}" -h "${mysql_host}" -D "${db}" < "${sql_file}" > "${entry_dir}/tidb.q${query_number}.result"
		local end_time=`date +%s`
		local elapsed="$((end_time - start_time))"
		local elapsed="${elapsed}	cat:${cat_tag},scale:${scale},query:${query_number},start_ts:${start_time},end_ts:${end_time}"
		# TODO: check result
		echo "${elapsed},${tags}" >> "${entry_dir}/queries.data"
	fi
}

function test_cluster_spark_run_tpch()
{
	if [ -z "${4+x}" ]; then
		echo "[func test_cluster_spark_run_tpch] usage: <func> test_ti_file scale entry_dir tags [query_number=all]"  >&2
		return 1
	fi

	local test_ti_file="${1}"
	local scale="${2}"
	local entry_dir="${3}"
	local tags="${4}"

	if [ ! -z "${5+x}" ]; then
		local query_number="${5}"
	else
		local query_number=''
	fi

	local db=`_db_name_from_scale "${scale}"`
	local queries_dir="${integrated}/resource/tpch/spark/queries"

	if [ ! -d "${queries_dir}" ]; then
		echo "[func test_cluster_spark_run_tpch] '${queries_dir}' not a dir" >&2
		return 1
	fi

	local ti="${integrated}/ops/ti.sh"
	if [ -z "${query_number}" ]; then
		for ((i = 1; i < 23; ++i)); do
			test_cluster_spark_run_tpch "${test_ti_file}" "${scale}" "${entry_dir}" "${tags}" "${i}"
		done
	else
		local sql_file="${queries_dir}/q${query_number}.sql"
		if [ ! -f "${sql_file}" ]; then
			echo "[func test_cluster_spark_run_tpch] '${sql_file}' not exists" >&2
			return 1
		fi
		echo "=> tpch on spark, scale ${scale}, query #${query_number}"
		local start_time=`date +%s`
		"${ti}" "${test_ti_file}" beeline "${db}" -f "${sql_file}" > "${entry_dir}/spark.q${query_number}.result" 2>&1
		local end_time=`date +%s`
		local elapsed="$((end_time - start_time))"
		local elapsed="${elapsed}	cat:spark,scale:${scale},query:${query_number},start_ts:${start_time},end_ts:${end_time}"
		# TODO: check result
		echo "${elapsed},${tags}" >> "${entry_dir}/queries.data"
	fi
}

function sleep_by_scale()
{
	if [ -z "${1+x}" ]; then
		echo "[func sleep_by_scale] usage: <func> scale" >&2
		return 1
	fi

	local scale="${1}"
	local has_dot=`echo "${scale}" | grep '\.'`
	if [ ! -z "${has_dot}" ]; then
		local scale='0'
	fi
	local sec=$((scale * 5 + 3))
	sleep "${sec}"
}

function tpch_perf_report()
{
	if [ -z "${2+x}" ]; then
		echo "[func tpch_perf_report] usage: <func> test_entry_file scale" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local scale="${2}"

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"

	local report="${entry_dir}/report"
	local title='            '

	rm -f "${report}.tmp"

	if [ -f "${entry_dir}/load.data" ]; then
		echo "<tpch ${scale} loading time>" >> "${report}.tmp"
		to_table "${title}" 'rows:; cols:table|notag; cell:limit(20)|avg|~|cnt|duration' 9999 "${entry_dir}/load.data" >> "${report}.tmp"
	fi

	if [ -f "${entry_dir}/queries.data" ]; then
		if [ -f "${entry_dir}/load.data" ]; then
			echo '' >> "${report}.tmp"
		fi
		echo "<tpch ${scale} execute time>" >> "${report}.tmp"
		to_table "${title}" 't=cat|r=round; cols:t,r; rows:query; cell:limit(20)|avg|~|cnt|duration' 9999 "${entry_dir}/queries.data" >> "${report}.tmp"
	fi

	if [ -f "${report}.tmp" ]; then
		mv -f "${report}.tmp" "${report}"
	fi
}

function tpch_perf()
{
	if [ -z "${2+x}" ]; then
		echo "[func tpch_perf] usage: <func> test_entry_file ports [scale]" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local ports="${2}"

	if [ ! -z "${3+x}" ]; then
		local scale="${3}"
	else
		local scale='1'
	fi

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"
	local report="${entry_dir}/report"

	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_prepare "${ports}" '' "${test_ti_file}"
	local vers=`test_cluster_vers "${test_ti_file}"`
	if [ -z "${vers}" ]; then
		echo "[func tpch_perf] test cluster prepare failed" >&2
		return 1
	else
		echo "[func tpch_perf] test cluster prepared: ${vers}"
	fi

	echo '---'

	test_cluster_load_tpch_data "${test_ti_file}" "${scale}" "${entry_dir}/load.data" "${vers}" | while read line; do
		tpch_perf_report "${test_entry_file}" "${scale}"
		echo "${line}"
	done
	echo "[func tpch_perf] load tpch data to test cluster done"

	echo '---'

	# TODO: remove this
	"${integrated}/ops/ti.sh" "${test_ti_file}" "ch" "DBGInvoke refresh_schemas()"

	for ((r = 0; r < 3; ++r)); do
		for ((i = 1; i < 23; ++i)); do
			test_cluster_run_tpch "${test_ti_file}" "${scale}" "${entry_dir}" "round:${r},${vers}" "${i}" 'false'
			tpch_perf_report "${test_entry_file}" "${scale}"
			test_cluster_run_tpch "${test_ti_file}" "${scale}" "${entry_dir}" "round:${r},${vers}" "${i}" 'true'
			tpch_perf_report "${test_entry_file}" "${scale}"
			test_cluster_spark_run_tpch "${test_ti_file}" "${scale}" "${entry_dir}" "round:${r},${vers}" "${i}"
			tpch_perf_report "${test_entry_file}" "${scale}"
		done
		sleep_by_scale "${scale}"
	done

	tpch_perf_report "${test_entry_file}" "${scale}"

	"${integrated}/ops/ti.sh" "${test_ti_file}" "burn" "doit"
	echo '[func tpch_perf] done'
}
