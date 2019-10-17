#!/bin/bash

function _db_name_from_scale()
{
	local scale="${1}"
	echo "tpch_${scale}" | tr '.' '_'
}
export -f _db_name_from_scale

function test_cluster_tmpl()
{
	echo "${integrated}/tests/_base/local_templ.ti"
}
export -f test_cluster_tmpl

function test_cluster_multi_node_tmpl()
{
	echo "${integrated}/tests/_base/multi_node_templ.ti"
}
export -f test_cluster_multi_node_tmpl

function test_cluster_args()
{
	if [ -z "${1+x}" ]; then
		echo "[func test_cluster_args] usage: <func> ports" >&2
		return 1
	fi

	local ports="${1}"

	if [ -z "${test_cluster_data_dir+x}" ] || [ -z "${test_cluster_data_dir}" ]; then
		echo "[func test_cluster_args] var 'test_cluster_data_dir' not defined" >&2
		return 1
	fi

	echo "ports=+${ports}#dir=${test_cluster_data_dir}/nodes/${ports}"
}
export -f test_cluster_args

function test_cluster_multi_node_args()
{
	if [ -z "${6+x}" ]; then
		echo "[func test_cluster_multi_node_args] usage: <func> ip1 ip2 ip3 ports1 ports2 ports3" >&2
		return 1
	fi

	local ip1="${1}"
	local ip2="${2}"
	local ip3="${3}"
	local ports1="${4}"
	local ports2="${5}"
	local ports3="${6}"

	if [ -z "${test_cluster_data_dir+x}" ] || [ -z "${test_cluster_data_dir}" ]; then
		echo "[func test_cluster_multi_node_args] var 'test_cluster_data_dir' not defined" >&2
		return 1
	fi

	echo "ports1=+${ports1}#ports2=+${ports2}#ports3=+${ports3}#ip1=${ip1}#ip2=${ip2}#ip3=${ip3}#dir1=${test_cluster_data_dir}/nodes/${ports1}#dir2=${test_cluster_data_dir}/nodes/${ports2}#dir3=${test_cluster_data_dir}/nodes/${ports3}"
}
export -f test_cluster_multi_node_args

function test_cluster_cmd()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_cmd] usage: <func> test_ti_file mods ops-ti-args" >&2
		return 1
	fi

	local test_ti_file="${1}"
	local mods="${2}"
	shift 2

	local ti="${integrated}/ops/ti.sh"

	"${ti}" -m "${mods}" "${test_ti_file}" "${@}"
}
export -f test_cluster_cmd

function test_cluster_ver()
{
	if [ -z "${2+x}" ]; then
		echo "[func test_cluster_ver] usage: <func> test_ti_file mod [mod_name_as_prefix=true]" >&2
		return 1
	fi

	local test_ti_file="${1}"
	local mod="${2}"
	if [ ! -z "${3+x}" ]; then
		local mod_name_prefix="${3}"
	else
		local mod_name_prefix='true'
	fi

	local ti="${integrated}/ops/ti.sh"

	local ver=`"${ti}" -m "${mod}" "${test_ti_file}"  ver ver`
	local failed=`echo "${ver}" | { grep 'unknown' || test $? = 1; }`
	if [ ! -z "${failed}" ]; then
		return 1
	fi

	if [ "${mod_name_prefix}" == 'true' ]; then
		local ver=`echo "${ver}" | awk '{print $1"_ver:"$2}'`
	else
		local ver=`echo "${ver}" | awk '{print "mod:"$1",ver:"$2}'`
	fi

	local githash=`"${ti}" -m "${mod}" "${test_ti_file}" ver githash`
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
		echo "[func test_cluster_vers] usage: <func> test_ti_file" >&2
		return 1
	fi

	local test_ti_file="${1}"

	# TODO: can be faster
	local version="`test_cluster_ver "${test_ti_file}" "pd"`,`test_cluster_ver "${test_ti_file}" "tikv"`,`test_cluster_ver "${test_ti_file}" "tidb"`,`test_cluster_ver "${test_ti_file}" "tiflash"`,`test_cluster_ver "${test_ti_file}" "rngine"`"
	echo "${version}"
}
export -f test_cluster_vers

function test_cluster_prepare()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_prepare] usage: <func> ports mods [rendered_file_path]" >&2
		return 1
	fi

	local ports="${1}"
	local mods="${2}"

	local rendered_file_path=''
	if [ ! -z "${3+x}" ]; then
		local rendered_file_path="${3}"
		local rendered_file_dir=`dirname "${rendered_file_path}"`
		local rendered_file_dir=`abs_path "${rendered_file_dir}"`
		local rendered_file_path=`basename "${rendered_file_path}"`
		local rendered_file_path="${rendered_file_dir}/${rendered_file_path}"
	fi

	local ti="${integrated}/ops/ti.sh"
	local args=`test_cluster_args "${ports}"`
	local ti_file=`test_cluster_tmpl`
	if [ ! -z "${rendered_file_path}" ]; then
		split_ti_args "${args}" > "${rendered_file_path}"
		echo '' >> "${rendered_file_path}"
		cat "${ti_file}" >> "${rendered_file_path}"
		"${ti}" "${rendered_file_path}" burn doit
		echo '---'
		"${ti}" -m "${mods}" "${rendered_file_path}" run
	else
		"${ti}" -k "${args}" "${ti_file}" burn doit
		echo '---'
		"${ti}" -m "${mods}" -k "${args}" "${ti_file}" run
	fi

	local status=`"${ti}" -m "${mods}" "${rendered_file_path}" status`
	local status_cnt=`echo "${status}" | wc -l | awk '{print $1}'`
	local ok_cnt=`echo "${status}" | { grep 'OK' || test $? = 1; } | wc -l | awk '{print $1}'`
	if [ "${status_cnt}" != "${ok_cnt}" ]; then
		echo "test cluster prepare failed, status:" >&2
		echo "${status}" >&2
		return 1
	fi
}
export -f test_cluster_prepare

function test_cluster_multi_node_prepare()
{
	if [ -z "${8+x}" ]; then
		echo "[func test_cluster_multi_node_prepare] usage: <func> ip1 ip2 ip3 ports1 ports2 ports3 mods [rendered_file_path]" >&2
		return 1
	fi

	local ip1="${1}"
	local ip2="${2}"
	local ip3="${3}"
	local ports1="${4}"
	local ports2="${5}"
	local ports3="${6}"
	local mods="${7}"

	local rendered_file_path=''
	if [ ! -z "${8+x}" ]; then
		local rendered_file_path="${8}"
		local rendered_file_dir=`dirname "${rendered_file_path}"`
		local rendered_file_dir=`abs_path "${rendered_file_dir}"`
		local rendered_file_path=`basename "${rendered_file_path}"`
		local rendered_file_path="${rendered_file_dir}/${rendered_file_path}"
	fi

	local ti="${integrated}/ops/ti.sh"
	local args=`test_cluster_multi_node_args "${ip1}" "${ip2}" "${ip3}" "${ports1}" "${ports2}" "${ports3}"`
	local ti_file=`test_cluster_multi_node_tmpl`
	if [ ! -z "${rendered_file_path}" ]; then
		split_ti_args "${args}" > "${rendered_file_path}"
		echo '' >> "${rendered_file_path}"
		cat "${ti_file}" >> "${rendered_file_path}"
		"${ti}" "${rendered_file_path}" burn doit
		echo '---'
		"${ti}" -m "${mods}" "${rendered_file_path}" run
	else
		"${ti}" -k "${args}" "${ti_file}" burn doit
		echo '---'
		"${ti}" -m "${mods}" -k "${args}" "${ti_file}" run
	fi

	local status=`"${ti}" -m "${mods}" "${rendered_file_path}" status`
	local status_cnt=`echo "${status}" | wc -l | awk '{print $1}'`
	local ok_cnt=`echo "${status}" | { grep 'OK' || test $? = 1; } | wc -l | awk '{print $1}'`
	if [ "${status_cnt}" != "${ok_cnt}" ]; then
		echo "test cluster prepare failed, status:" >&2
		echo "${status}" >&2
		return 1
	fi
}
export -f test_cluster_multi_node_prepare

function test_cluster_burn()
{
	if [ -z "${1+x}" ]; then
		echo "[func test_cluster_burn] usage: <func> test_ti_file" >&2
		return 1
	fi

	local test_ti_file="${1}"
	local ti="${integrated}/ops/ti.sh"
	"${ti}" "${test_ti_file}" burn doit
}
export -f test_cluster_burn

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
export -f _test_cluster_gen_and_load_tpch_table

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
export -f test_cluster_load_tpch_table

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
export -f test_cluster_load_tpch_data

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
export -f test_cluster_run_tpch

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
export -f test_cluster_spark_run_tpch

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
export -f sleep_by_scale

function load_tpcc_data()
{
	local benchmark_dir="${1}"
	local test_ti_file="${2}"
	local warehouses="${3}"
	local minutes="${4}"

	local ti="${integrated}/ops/ti.sh"

	local tidb_host=`"${ti}" "${test_ti_file}" "mysql/host"`
	local tidb_port=`"${ti}" "${test_ti_file}" "mysql/port"`
	local prop_file="${benchmark_dir}/props.mysql"

	"${ti}" "${test_ti_file}" "mysql" "create database tpcc"
	echo "db=mysql" > "${prop_file}"
	echo "driver=com.mysql.jdbc.Driver" >> "${prop_file}"
	echo "conn=jdbc:mysql://${tidb_host}:${tidb_port}/tpcc?useSSL=false&useServerPrepStmts=true&useConfigs=maxPerformance" >> "${prop_file}"
	echo "user=root" >> "${prop_file}"
	echo "password=" >> "${prop_file}"
	echo "warehouses=${warehouses}" >> "${prop_file}"
	echo "loadWorkers=4" >> "${prop_file}"
	echo "terminals=1" >> "${prop_file}"
	echo "runTxnsPerTerminal=0" >> "${prop_file}"
	echo "runMins=${minutes}" >> "${prop_file}"
	echo "limitTxnsPerMin=0" >> "${prop_file}"
	echo "terminalWarehouseFixed=true" >> "${prop_file}"
	echo "newOrderWeight=45" >> "${prop_file}"
	echo "paymentWeight=43" >> "${prop_file}"
	echo "orderStatusWeight=4" >> "${prop_file}"
	echo "deliveryWeight=4" >> "${prop_file}"
	echo "stockLevelWeight=4" >> "${prop_file}"
	echo "resultDirectory=my_result_%tY-%tm-%td_%tH%tM%tS" >> "${prop_file}"

	(
		cd "${benchmark_dir}"
		./runSQL.sh props.mysql sql.mysql/tableCreates.sql 2>&1 1>/dev/null
		./runSQL.sh props.mysql sql.mysql/indexCreates.sql 2>&1 1>/dev/null
		./runLoader.sh props.mysql 2>&1
	)
	wait
}
export -f load_tpcc_data

function test_cluster_run_tpcc()
{
	local type="${1}"
	local benchmark_dir="${2}"
	local entry_dir="${3}"
	local tags="${4}"

	local start_time=`date +%s`
	(
		cd "${benchmark_dir}"
		./runBenchmark.sh props.mysql 2>&1 1>"./test.log"
	)
	wait
	local end_time=`date +%s`
	local tags="type:${type},test:tpcc,start_ts:${start_time},end_ts:${end_time},${tags}"
	local result=`grep "Measured tpmC" "${benchmark_dir}/test.log" | awk -F '=' '{print $2}' | tr -d ' ' | awk -F '.' '{print $1}'`
	echo "${result} ${tags}" >> "${entry_dir}/results.data"
}
export -f test_cluster_run_tpcc

function test_cluster_get_normal_store_count()
{
	if [ -z "${1+x}" ]; then
		echo "[func test_cluster_get_normal_store_count] usage: <func> test_ti_file"  >&2
		return 1
	fi
	local test_ti_file="${1}"

	local store_count=`"${integrated}/ops/ti.sh" "${test_ti_file}" "pd_ctl" "store" | { grep '"role": "normal"' || test $? = 1; } | wc -l`
	if [ -z "${store_count}" ]; then
		echo "[func test_cluster_get_normal_store_count] get store count failed" >&2
		return 1
	fi
	echo "${store_count}"
}
export -f test_cluster_get_normal_store_count

function test_cluster_get_learner_store_count()
{
	if [ -z "${1+x}" ]; then
		echo "[func test_cluster_get_learner_store_count] usage: <func> test_ti_file"  >&2
		return 1
	fi
	local test_ti_file="${1}"

	local store_count=`"${integrated}/ops/ti.sh" "${test_ti_file}" "pd_ctl" "store" | { grep '"role": "slave"' || test $? = 1; } | wc -l`
	if [ -z "${store_count}" ]; then
		echo "[func test_cluster_get_learner_store_count] get store count failed" >&2
		return 1
	fi
	echo "${store_count}"
}
export -f test_cluster_get_learner_store_count

function test_cluster_get_store_id()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_get_store_id] usage: <func> test_ti_file store_ip store_port"  >&2
		return 1
	fi
	local test_ti_file="${1}"
	local store_ip="${2}"
	local store_port="${3}"

	local store_address="${store_ip}:${store_port}"
	local store_id=`"${integrated}/ops/ti.sh" "${test_ti_file}" "pd_ctl" "store" | { grep -B 2 "${store_address}" || test $? = 1; } | { grep "id" || test $? = 1; } | awk -F ':' '{print $2}' | tr -cd '[0-9]'`
	if [ -z "${store_id}" ]; then
		echo "[func test_cluster_get_store_id] cannot get store ${store_address} store id" >&2
		return 1
	fi
	echo "${store_id}"
}
export -f test_cluster_get_store_id

function test_cluster_get_store_region_count()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_get_store_region_count] usage: <func> test_ti_file store_ip store_port"  >&2
		return 1
	fi
	local test_ti_file="${1}"
	local store_ip="${2}"
	local store_port="${3}"

	local store_id=`test_cluster_get_store_id "${test_ti_file}" "${store_ip}" "${store_port}"`
	local region_count=`"${integrated}/ops/ti.sh" "${test_ti_file}" "pd_ctl" "store ${store_id}" | { grep "region_count" || test $? = 1; } | awk -F ':' '{print $2}' | tr -cd '[0-9]'`
	if [ -z "${region_count}" ]; then
		echo "[func test_cluster_get_store_region_count] cannot get store ${store_ip}:${store_port} store_id ${store_id} region count" >&2
		local region_count=0
	fi
	echo "${region_count}"
}
export -f test_cluster_get_store_region_count

function test_cluster_remove_store()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_remove_store] usage: <func> test_ti_file store_ip store_port"  >&2
		return 1
	fi
	local test_ti_file="${1}"
	local store_ip="${2}"
	local store_port="${3}"

	local store_id=`test_cluster_get_store_id "${test_ti_file}" "${store_ip}" "${store_port}"`
	local op_response=`"${integrated}/ops/ti.sh" "${test_ti_file}" "pd_ctl" "store delete ${store_id}" | tr -d '\[\][0-9] \.'`
	if [ "${op_response}" != "Success!" ]; then
		echo "[func test_cluster_remove_store] remove store ${store_ip}:${store_port} failed"
		return 1
	fi
}
export -f test_cluster_remove_store
