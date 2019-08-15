#!/bin/bash

# Where to launch tiflash
tiflash_dir="${1}"

# Specify tiflash listening ports
ports_delta="${2}"

# Where to find pd
pd_addr="${3}"

# Run as daemon or not
daemon_mode="${4}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"
auto_error_handle

if [ -z "${tiflash_dir}" ]; then
	echo "[script tiflash_run] usage: <script> tiflash_dir [ports_delta=0] [pd_addr=''] [daemon=true]" >&2
	exit 1
fi

if [ -z "${ports_delta}" ]; then
	ports_delta="0"
fi

if [ -z "${pd_addr}" ]; then
	pd_addr=""
fi

if [ -z "${daemon_mode}" ]; then
	daemon_mode="true"
fi

# Where is tiflash config template files
conf_templ_dir="${here}/conf_templ"

source "${conf_templ_dir}/default_ports.sh"

tiflash_run "${tiflash_dir}" "${conf_templ_dir}" "${daemon_mode}" "${pd_addr}" "${ports_delta}"
