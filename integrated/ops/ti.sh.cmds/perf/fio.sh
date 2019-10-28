#!/bin/bash

index="${1}"
mod_name="${2}"
dir="${3}"
conf_rel_path="${4}"
host="${5}"
shift 5

cmd_args=("${@}")
set -euo pipefail

mkdir -p "${dir}/tmp"

echo "=> ${mod_name} #${index} ${dir}"
fio -filename="${dir}/tmp/fio.tmp" "${cmd_args}" | awk '{print "   "$0}'
