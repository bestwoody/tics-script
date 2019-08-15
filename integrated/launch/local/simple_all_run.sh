#!/bin/bash

# Where to launch
dir="${1}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"

if [ -z "${dir}" ]; then
	echo "[script simple_all_run] usage: <script> dir" >&2
	exit 1
fi

echo "=> pd"
"${here}/pd_run.sh" "${dir}/pd"

echo "=> tikv"
"${here}/tikv_run.sh" "${dir}/tikv"

echo "=> tidb"
"${here}/tidb_run.sh" "${dir}/tidb"

echo "=> tiflash"
wait_for_tidb "${dir}/tidb"
"${here}/tiflash_run.sh" "${dir}/tiflash"

echo "=> rngine"
"${here}/rngine_run.sh" "${dir}/rngine"
