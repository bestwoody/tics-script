#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

function test_raft_sync()
{
	local ti="${1}"
	local ti_file="${2}"
	local args="${3}"

	"${ti}" -k "${args}" "${ti_file}" burn doit
	"${ti}" -k "${args}" -m "pd,tikv,tidb,tiflash,rngine" "${ti_file}" 'run'

	local tidb_ok=`"${ti}" -k "${args}" "${ti_file}" 'mysql' "show databases" | grep 'test'`
	if [ ! -z "${tidb_ok}" ]; then
		echo 'tidb connectable'
	else
		echo 'tidb unconnectable'
		return 1
	fi

	local tiflash_ok=`"${ti}" -k "${args}" "${ti_file}" 'ch' "show databases" | grep 'default'`
	if [ ! -z "${tiflash_ok}" ]; then
		echo 'tiflash connectable'
	else
		echo 'tiflash unconnectable'
		return 1
	fi

	"${ti}" -k "${args}" "${ti_file}" 'mysql' "create table test.hello (col varchar(10))"
	local create_ok=`"${ti}" -k "${args}" "${ti_file}" 'mysql' "show create table test.hello" 2>&1 | grep 'InnoDB'`
	if [ ! -z "${create_ok}" ]; then
		echo "create ok"
	else
		echo "create failed: ${create_ok}"
		return 1
	fi

	"${ti}" -k "${args}" "${ti_file}" 'mysql' "insert into test.hello (col) values ('world')"
	local insert_ok=`"${ti}" -k "${args}" "${ti_file}" 'mysql' "select col from test.hello" 2>&1 | grep 'world'`
	if [ ! -z "${insert_ok}" ]; then
		echo "insert ok"
	else
		echo "insert failed: ${create_ok}"
		return 1
	fi

	local sync_err=`"${ti}" -k "${args}" "${ti_file}" 'ch' "select col from hello" 'test' '' --schema_version 10000000 2>&1 | grep 'Exception'`
	if [ -z "${sync_err}" ]; then
		echo "data sync ok"
	else
		echo "data sync failed: ${sync_err}"
		return 1
	fi

	"${ti}" -k "${args}" "${ti_file}" burn doit
}

test_raft_sync "${integrated}/ops/ti.sh" "${integrated}/tests/_base/local_templ.ti" "ports=+7#dir=nodes/7"
