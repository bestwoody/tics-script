#!/bin/bash

# Where to launch
dir="${1}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"

if [ -z "${dir}" ]; then
	echo "[script simple_all_run] usage: <script> dir" >&2
	exit 1
fi

echo "=> rngine"
"${here}/rngine_stop.sh" "${dir}/rngine"
echo "=> tiflash"
"${here}/tiflash_stop.sh" "${dir}/tiflash"
echo "=> tidb"
"${here}/tidb_stop.sh" "${dir}/tidb"
echo "=> tikv"
"${here}/tikv_stop.sh" "${dir}/tikv"
echo "=> pd"
"${here}/pd_stop.sh" "${dir}/pd"
