#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

function stop_mod()
{
	local mod="${1}"
	local entry_dir="${2}"
	local test_ti_file="${3}"
	local start_time=`date +%s`
	local ti="${integrated}/ops/ti.sh"
	"${ti}" -m "${mod}" "${test_ti_file}" "stop"
	local end_time=`date +%s`
	local tags="op:stop,start_ts:${start_time},end_ts:${end_time},`get_mod_ver "${mod}" "${test_ti_file}"`"
	echo "$((end_time - start_time)) ${tags}" >> "${entry_dir}/data"
}

function run_mod()
{
	local mod="${1}"
	local entry_dir="${2}"
	local test_ti_file="${3}"
	local start_time=`date +%s`

	local ti="${integrated}/ops/ti.sh"
	"${ti}" -m "${mod}" "${test_ti_file}" "run"
	local end_time=`date +%s`
	local status=`"${ti}" -m "${mod}" "${test_ti_file}" "status"`
	local ok=`echo "${status}" | grep 'OK'`
	if [ -z "${ok}" ]; then
		echo "${status}" >&2
		return 1
	fi
	local tags="op:run,start_ts:${start_time},end_ts:${end_time},`get_mod_ver "${mod}" "${test_ti_file}"`"
	echo "$((end_time - start_time)) ${tags}" >> "${entry_dir}/data"
}

function test_mods()
{
	if [ -z "${2+x}" ]; then
		echo "[func test_mods] usage: <func> test_entry_file ports" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local ports="${2}"

	local entry_dir=`get_test_entry_dir "${test_entry_file}"`
	mkdir -p "${entry_dir}"
	local title='<cluster run/stop elapsed>'
	local report="${entry_dir}/report"

	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_prepare "${ports}" "pd,tikv,tidb,tiflash,rngine" "${test_ti_file}"

	"${integrated}/ops/ti.sh" "${test_ti_file}" "stop"

	run_mod 'pd' "${entry_dir}" "${test_ti_file}"
	run_mod 'tikv' "${entry_dir}" "${test_ti_file}"
	run_mod 'tidb' "${entry_dir}" "${test_ti_file}"
	run_mod 'tiflash' "${entry_dir}" "${test_ti_file}"
	run_mod 'rngine' "${entry_dir}" "${test_ti_file}"
	stop_mod 'pd' "${entry_dir}" "${test_ti_file}"
	stop_mod 'tikv' "${entry_dir}" "${test_ti_file}"
	stop_mod 'tidb' "${entry_dir}" "${test_ti_file}"
	stop_mod 'tiflash' "${entry_dir}" "${test_ti_file}"
	stop_mod 'rngine' "${entry_dir}" "${test_ti_file}"

	to_table "${title}" 'cols:op; rows:mod,ver|notag; cell:limit(20)|avg|~|duration' 9999 "${entry_dir}/data" > "${report}.tmp"
	mv -f "${report}.tmp" "${report}"
	echo 'done'
}

test_mods "${BASH_SOURCE[0]}" 3
