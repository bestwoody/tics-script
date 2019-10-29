#!/bin/bash

index="${1}"
mod_name="${2}"
dir="${3}"
conf_rel_path="${4}"
host="${5}"
shift 5

if [ "${mod_name}" != 'tiflash' ]; then
	exit
fi

db_name="$1"
table="$2"

set -euo pipefail

if [ -z "$table" ]; then
	echo "[cmd ch/analyze_compaction] usage: <cmd> database table" >&2
	exit 1
fi

echo "=> ${mod_name} #${index} ${dir}"
if [ ! -d "${dir}" ]; then
	echo "   missed"
	exit
fi

config="${dir}/conf/config.xml"
if [ ! -f "${config}" ]; then
	echo "   error: config file missed"
	exit
fi

data_path=`grep '<path>' "${config}" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
table_path="${dir}/db/data/${db_name}/${table}"
if [ ! -d "${table_path}" ]; then
	echo "   error: table ${db_name}.${table} missed"
	exit
fi

curr_path=`pwd`
cd "${table_path}"

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
du -sk * | python "${here}/analyze_compaction.py" | awk '{print "   "$0}'

cd "${curr_path}"
