#!/bin/bash

# The host to launch tiflash
host="${1}"
# Where to launch tiflash
tiflash_dir="${2}"
# Specify tiflash listening ports
ports_delta="${3}"
# Where to find pd
pd_addr="${4}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"
auto_error_handle

if [ -z "${host}" ] || [ -z "${tiflash_dir}" ]; then
	echo "[script tiflash_run] usage: <script> host tiflash_dir [ports_delta=0] [pd_addr='']" >&2
	exit 1
fi

# Where is tiflash config template files
conf_templ_dir=`dirname "${here}"`/local/conf_templ

cache_root="/tmp/ti/integrated"
cache_dir="${cache_root}/master/bins"
remote_env="${cache_root}/worker/integrated"

ssh_ping "${host}"
cp_env_to_host "${integrated}" "${cache_root}/master/integrated" "${host}" "${cache_root}/worker"
cp_bin_to_host "tiflash" "${host}" "${remote_env}" "${cache_root}/worker/bins" "${conf_templ_dir}/bin.paths" "${conf_templ_dir}/bin.urls" "${cache_dir}"

call_remote_func "${host}" "${remote_env}" cp_bin_to_dir "tiflash" "${tiflash_dir}" "${remote_env}/conf/bin.paths" "${remote_env}/conf/bin.urls" "${cache_dir}"
call_remote_func "${host}" "${remote_env}" tiflash_run "${tiflash_dir}" "${remote_env}/conf" "true" "${pd_addr}" "${ports_delta}" "${host}"
