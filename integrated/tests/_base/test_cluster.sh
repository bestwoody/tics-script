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

function test_cluster_get_normal_store_count()
{
	if [ -z "${1+x}" ]; then
		echo "[func test_cluster_get_normal_store_count] usage: <func> test_ti_file"  >&2
		return 1
	fi
	local test_ti_file="${1}"

	local store_count=`"${integrated}/ops/ti.sh" -i 0 "${test_ti_file}" "pd/ctl" "store" | { grep '"role": "normal"' || test $? = 1; } | wc -l`
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

	local store_count=`"${integrated}/ops/ti.sh" -i 0 "${test_ti_file}" "pd/ctl" "store" | { grep '"role": "slave"' || test $? = 1; } | wc -l`
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
	local store_id=`"${integrated}/ops/ti.sh" -i 0 "${test_ti_file}" "pd/ctl" "store" | { grep -B 2 "${store_address}" || test $? = 1; } | { grep "id" || test $? = 1; } | awk -F ':' '{print $2}' | tr -cd '[0-9]'`
	if [ -z "${store_id}" ]; then
		echo "[func test_cluster_get_store_id] cannot get store ${store_address} store id" >&2
		return 1
	fi
	echo "${store_id}"
}
export -f test_cluster_get_store_id

function test_cluster_get_store_id_by_index()
{
	if [ -z "${3+x}" ]; then
		echo "[func test_cluster_get_store_id_by_index] usage: <func> test_ti_file index module"  >&2
		return 1
	fi
	local test_ti_file="${1}"
	local index="${2}"
	local module="${3}"

	local prop=`"${ti}" -i "${index}" -m "${module}" "${test_ti_file}" 'prop'`
	local host=`echo "${prop}" | grep "listen_host" | awk -F 'listen_host:' '{print $2}' | tr -d ' '`
	if [ "${module}" == "rngine" ]; then
		local port=`echo "${prop}" | grep "rngine_port" | awk -F 'rngine_port:' '{print $2}' | tr -d ' '`
	else
		local port=`echo "${prop}" | grep "tikv_port" | awk -F 'tikv_port:' '{print $2}' | tr -d ' '`
	fi

	local store_id=`test_cluster_get_store_id "${test_ti_file}" "${host}" "${port}"`
	if [ -z "${store_id}" ]; then
		echo "[func test_cluster_get_store_id_by_index] cannot get ${index} of ${module} store id" >&2
		return 1
	fi
	echo "${store_id}"
}
export -f test_cluster_get_store_id_by_index

function test_cluster_get_learner_store_id_by_index()
{
	local test_ti_file="${1}"
	local index="${2}"
	
	echo `test_cluster_get_store_id_by_index "${test_ti_file}" "${index}" "rngine"`
}
export -f test_cluster_get_learner_store_id_by_index

function test_cluster_get_normal_store_id_by_index()
{
	local test_ti_file="${1}"
	local index="${2}"
	
	echo `test_cluster_get_store_id_by_index "${test_ti_file}" "${index}" "tikv"`
}
export -f test_cluster_get_normal_store_id_by_index

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
	local region_count=`"${integrated}/ops/ti.sh" -i 0 "${test_ti_file}" "pd/ctl" "store ${store_id}" | { grep "region_count" || test $? = 1; } | awk -F ':' '{print $2}' | tr -cd '[0-9]'`
	if [ -z "${region_count}" ]; then
		echo "[func test_cluster_get_store_region_count] cannot get store ${store_ip}:${store_port} store_id ${store_id} region count" >&2
		local region_count=0
	fi
	echo "${region_count}"
}
export -f test_cluster_get_store_region_count

function test_cluster_get_cluster_region_count()
{
	if [ -z "${1+x}" ]; then
		echo "[func test_cluster_get_cluster_region_count] usage: <func> test_ti_file"  >&2
		return 1
	fi
	local test_ti_file="${1}"

	local ti="${integrated}/ops/ti.sh"
	local store_count=`test_cluster_get_normal_store_count "${test_ti_file}"`
	local cluster_region_count=0
	for ((index=0; index<${store_count}; index++)); do
		local prop=`"${ti}" -i "${index}" -m "tikv" "${test_ti_file}" 'prop'`
		local host=`echo "${prop}" | grep "listen_host" | awk -F 'listen_host:' '{print $2}' | tr -d ' '`
		local port=`echo "${prop}" | grep "tikv_port" | awk -F 'tikv_port:' '{print $2}' | tr -d ' '`
		local store_region_count=`test_cluster_get_store_region_count "${test_ti_file}" "${host}" "${port}"`
		local cluster_region_count=$(( cluster_region_count + store_region_count ))
	done

	echo "${cluster_region_count}"
}
export -f test_cluster_get_cluster_region_count

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
	local op_response=`"${integrated}/ops/ti.sh" -i 0 "${test_ti_file}" "pd/ctl" "store delete ${store_id}" | tr -d '\[\][0-9] \.'`
	if [ "${op_response}" != "Success!" ]; then
		echo "[func test_cluster_remove_store] remove store ${store_ip}:${store_port} failed"
		return 1
	fi
}
export -f test_cluster_remove_store
