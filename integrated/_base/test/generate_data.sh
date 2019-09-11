#!/bin/bash

function download_dbgen()
{
	local dbgen_url="${1}"
	local dbgen_bin_dir="${2}"
	if [ -z "${dbgen_url}" ] || [ -z "${dbgen_bin_dir}" ]; then
		echo "[func download_dbgen] usage: <func> dbgen_url dbgen_bin_dir"
		return 1
	fi
	mkdir -p "${dbgen_bin_dir}"
	local download_path="${dbgen_bin_dir}/dbgen.tar.gz.`date +%s`.${RANDOM}"
	if [ ! -f "${dbgen_bin_dir}/dbgen.tar.gz" ]; then
		wget --quiet -nd "${dbgen_url}" -O "${download_path}"
		mv "${download_path}" "${dbgen_bin_dir}/dbgen.tar.gz"
	fi
	if [ ! -f "${dbgen_bin_dir}/dbgen" ]; then
		tar -zxf "${dbgen_bin_dir}/dbgen.tar.gz" -C "${dbgen_bin_dir}"
	fi
}
export -f download_dbgen

function table_to_arguments()
{
	local table="${1}"
	if [ -z "${table}" ]; then
		echo "[func table_to_arguments] usage: <func> table_name"
		return 1
	fi
	if [ "${table}" == "lineitem" ]; then
		echo "L"
	fi
	if [ "${table}" == "partsupp" ]; then
		echo "S"
	fi
	if [ "${table}" == "customer" ]; then
		echo "c"
	fi
	if [ "${table}" == "nation" ]; then
		echo "n"
	fi
	if [ "${table}" == "orders" ]; then
		echo "O"
	fi
	if [ "${table}" == "part" ]; then
		echo "P"
	fi
	if [ "${table}" == "region" ]; then
		echo "r"
	fi
	if [ "${table}" == "supplier" ]; then
		echo "s"
	fi
}
export -f table_to_arguments

function generate_tpch_data_to_dir()
{
	local dbgen_bin_dir="${1}"
	local data_dir="${2}"
	local scale="${3}"
	local table="${4}"
	local blocks="${5}"
	if [ -z "${dbgen_bin_dir}" ] || [ -z "${data_dir}" ] || [ -z "${scale}" ] || [ -z "${table}" ] || [ -z "${blocks}" ]; then
		echo "[func generate_tpch_data_to_dir] usage: <func> dbgen_bin_dir data_dir scale table blocks"
		return 1
	fi

	mkdir -p "${data_dir}"
	(
		cd "${data_dir}"
		for ((i=1; i<${blocks}+1; ++i)); do
			"${dbgen_bin_dir}/dbgen" -C "${blocks}" -T `table_to_arguments "${table}"` -s "${scale}" -S "${i}" -f &
		done
		wait
	)
	wait
}
export -f generate_tpch_data_to_dir

function generate_tpch_data()
{
	local dbgen_url="${1}"
	local dbgen_bin_dir="${2}"
	local data_dir="${3}"
	local scale="${4}"
	local table="${5}"
	local blocks="${6}"
	local dists_dss_url="${7}"

	if [ -z "${dbgen_url}" ] || [ -z "${dbgen_bin_dir}" ] || [ -z "${data_dir}" ] || [ -z "${scale}" ] || [ -z "${table}" ] || [ -z "${blocks}" ] || [ -z "${dists_dss_url}" ]; then
		echo "[func generate_tpch_data] usage: <func> dbgen_url dbgen_bin_dir data_dir scale table blocks dists_dss_url"
		return 1
	fi

	download_dbgen "${dbgen_url}" "${dbgen_bin_dir}"

	if [ -d "${data_dir}" ]; then
		echo "data dir ${data_dir} exists."
		return 0
	fi

	local temp_data_dir="${data_dir}_`date +%s`.${RANDOM}"
	mkdir -p "${temp_data_dir}"
	if [ ! -f "${temp_data_dir}/dists.dss" ]; then
		wget --quiet -nd "${dists_dss_url}" -O "${temp_data_dir}/dists.dss"
	fi
	generate_tpch_data_to_dir "${dbgen_bin_dir}" "${temp_data_dir}" "${scale}" "${table}" "${blocks}"
	local data_file_count=`ls "${temp_data_dir}" | grep "${table}" | wc -l`
	if [ "${data_file_count}" <= 0 ]; then
		echo "[func generate_tpch_data] generate data file failed"
		return 1
	fi
	mv "${temp_data_dir}" "${data_dir}"
}
export -f generate_tpch_data
