#!/bin/bash

path="${1}"
tiflash_num="${2}"
tikv_num="${3}"
data_dir="${4}"

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

if [ -z "${path}" ]; then
	echo "[cmd new] usage: <cmd> file_path_to_be_created.ti [tiflash_num=1] [tikv_num=1] [data_dir=nodes/n0]"
	exit 1
fi
if [ -z "${tiflash_num}" ]; then
	tiflash_num='1'
fi
if [ -z "${tikv_num}" ]; then
	tikv_num='1'
fi
if [ -z "${data_dir}" ]; then
	data_dir='nodes/n0'
fi

ext=`print_file_ext "${path}"`
if [ "${ext}" != 'ti' ]; then
	echo "[cmd new] the file name must have '.ti' suffix"
	exit 1
fi

echo "dir=${data_dir}" > "${path}"
echo "delta=0" >> "${path}"
echo >> "${path}"
echo "pd: {dir}/pd ports+{delta}" >> "${path}"
echo "tidb: {dir}/tidb ports+{delta}" >> "${path}"
echo >> "${path}"
for (( i = 0; i < ${tikv_num}; i++ )); do
	echo "tikv: {dir}/tikv${i} ports+{delta}+${i}" >> "${path}"
done
echo >> "${path}"
for (( i = 0; i < ${tiflash_num}; i++ )); do
	d=$((i * 2))
	echo "tiflash: {dir}/tiflash${i} ports+{delta}+${d}" >> "${path}"
done
echo >> "${path}"
for (( i = 0; i < ${tiflash_num}; i++ )); do
	d=$((i * 2))
	echo "rngine: {dir}/rngine${i} tiflash={dir}/tiflash${i} ports+{delta}+${d}" >> "${path}"
done

echo "${path} created"
