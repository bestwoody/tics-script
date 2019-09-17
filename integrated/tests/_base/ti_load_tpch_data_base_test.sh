#!/bin/bash

function load_and_generate_tpch_data()
{
	local schema_dir="${1}"
	local data_dir="${2}"
	local scale="${3}"
	local table="${4}"
	local blocks="${5}"
	local ti_file="${6}"
	local ti_file_args="${7}"

	local db="tpch${scale}"

	# TODO: remove hard code url
	if [ `uname` == "Darwin" ]; then
		local dbgen_url="http://139.219.11.38:8000/3GdrI/dbgen.tar.gz"
	else
		local dbgen_url="http://139.219.11.38:8000/fCROr/dbgen.tar.gz"
	fi

	local dists_dss_url="http://139.219.11.38:8000/v2TLJ/dists.dss"

	local dbgen_bin_dir="/tmp/ti/master/bins"

	generate_tpch_data "${dbgen_url}" "${dbgen_bin_dir}" "${data_dir}/tpch${scale}_${blocks}/${table}" "${scale}" "${table}" "${blocks}" "${dists_dss_url}"
	load_tpch_data_to_ti_cluster "${ti_file}" "${schema_dir}" "${data_dir}/tpch${scale}_${blocks}/${table}" "${db}" "${table}" "${ti_file_args}"
}
export -f load_and_generate_tpch_data

function load_tpch_data_base_test()
{
	local schema_dir="${1}"
	local data_dir="${2}"
	local scale="${3}"
	local table="${4}"
	local ti_sh="${5}"
	local ti_file="${6}"
	local ti_file_args="${7}"

	local db="tpch${scale}"

	local blocks="4"

	"${ti_sh}" -k "${ti_file_args}" "${ti_file}" burn doit
	"${ti_sh}" -k "${ti_file_args}" "${ti_file}" 'run'

	load_and_generate_tpch_data "${schema_dir}" "${data_dir}" "${scale}" "${table}" "${blocks}" "${ti_file}" "${ti_file_args}"

	local status=`"${ti_sh}" -k "${ti_file_args}" "${ti_file}" 'status'`
	local ok=`echo "${status}" | grep 'OK' | wc -l`
	if [ "${ok}" != "5" ]; then
		echo "${status}" >&2
		"${ti_sh}" -k "${ti_file_args}" "${ti_file}" burn doit
		return 1
	fi
	"${ti_sh}" -k "${ti_file_args}" "${ti_file}" burn doit
}
export -f load_tpch_data_base_test