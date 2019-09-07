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

function get_ver()
{
	local mod="${1}"
	local ver=`"${ti}" -k "${args}" -m "${mod}" "${ti_file}" ver ver | awk '{print "mod:"$1",ver:"$2}'`
	local failed=`echo "${ver}" | grep 'unknown'`
	if [ ! -z "${failed}" ]; then
		return 1
	fi
	local git=`"${ti}" -k "${args}" -m "${mod}" "${ti_file}" ver githash | awk '{print "git:"$2}'`
	echo "${ver},${git}"
}

function stop_mod()
{
	local mod="${1}"
	local start_time=`date +%s`
	"${ti}" -k "${args}" -m "${mod}" "${ti_file}" 'stop'
	local end_time=`date +%s`
	local tags="op:stop,start_ts:${start_time},end_ts:${end_time},`get_ver "${mod}"`"
	echo "$((end_time - start_time)) ${tags}" >> "${data}"
}

function run_mod()
{
	local mod="${1}"
	local start_time=`date +%s`
	"${ti}" -k "${args}" -m "${mod}" "${ti_file}" 'run'
	local end_time=`date +%s`
	local status=`"${ti}" -k "${args}" -m "${mod}" "${ti_file}" 'status'`
	local ok=`echo "${status}" | grep 'OK'`
	if [ -z "${ok}" ]; then
		echo "${status}" >&2
		return 1
	fi
	local tags="op:run,start_ts:${start_time},end_ts:${end_time},`get_ver "${mod}"`"
	echo "$((end_time - start_time)) ${tags}" >> "${data}"
}

function test_mods()
{
	"${ti}" -k "${args}" "${ti_file}" burn doit

	run_mod 'pd'
	run_mod 'tikv'
	run_mod 'tidb'
	run_mod 'tiflash'
	run_mod 'rngine'
	stop_mod 'pd'
	stop_mod 'tikv'
	stop_mod 'tidb'
	stop_mod 'tiflash'
	stop_mod 'rngine'

	"${ti}" -k "${args}" "${ti_file}" burn doit

	to_table "${title}" 'cols:op; rows:mod,ver|notag; cell:limit(20)|avg|~|duration' 9999 "${data}" > "${report}.tmp"
	mv -f "${report}.tmp" "${report}"
	echo 'done'
}

test_mods
