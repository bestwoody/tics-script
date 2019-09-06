#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1_x.ti"
args="ports=+3#dir=nodes/3"

title='<cluster run/stop elapsed>'
data="${BASH_SOURCE[0]}.data"
report="${BASH_SOURCE[0]}.report"

function test_mod()
{
	local mod="${1}"
	local op="${2}"

	local check_ok='true'
	if [ "${op}" == 'stop' ]; then
		local check_ok='false'
	fi

	local start_time=`date +%s`
	"${ti}" -k "${args}" -m "${mod}" "${ti_file}" "${op}"
	local end_time=`date +%s`

	if [ "${check_ok}" == 'true' ]; then
		local status=`"${ti}" -k "${args}" -m "${mod}" "${ti_file}" 'status'`
		local ok=`echo "${status}" | grep 'OK'`
		if [ -z "${ok}" ]; then
			echo "${status}" >&2
			return 1
		fi
	fi

	local ver=`"${ti}" -k "${args}" -m "${mod}" "${ti_file}" ver ver | awk '{print "mod:"$1",ver:"$2}'`
	local git=`"${ti}" -k "${args}" -m "${mod}" "${ti_file}" ver githash | awk '{print "git:"$2}'`

	local tags="op:${op},ts:${end_time},${ver},${git}"
	echo "$((end_time - start_time)) ${tags}" >> "${data}"
}

function test_mods()
{
	"${ti}" -k "${args}" "${ti_file}" burn doit

	test_mod 'pd' 'run'
	test_mod 'tikv' 'run'
	test_mod 'tidb' 'run'
	test_mod 'tiflash' 'run'
	test_mod 'rngine' 'run'
	test_mod 'pd' 'stop'
	test_mod 'tikv' 'stop'
	test_mod 'tidb' 'stop'
	test_mod 'tiflash' 'stop'
	test_mod 'rngine' 'stop'

	"${ti}" -k "${args}" "${ti_file}" burn doit

	to_table "${title}" 'cols:op; rows:mod,ver|notag; cell:limit(20)|avg|~|duration' 9999 "${data}" > "${report}"
	echo 'done'
}

test_mods
