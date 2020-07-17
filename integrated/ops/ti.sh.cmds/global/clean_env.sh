#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/../../_env.sh"
auto_error_handle

function kill_process() {
    if [ -z "${1+x}" ] || [ -z "${2+x}" ]; then
        echo '[cmd clean_env] usage: <cmd> process hint' >&2
        return 1
    fi
    local process="${1}"
    local hint="${2}"
    set +e
    echo "killing ${process}..."
    local pids=`ps -ef | grep "${process}" | grep "${hint}" | grep -v "grep" | grep -v "ti.sh" | awk -F ' ' '{print $2}'`
    if [ ! -z "${pids}" ]; then
        echo "${pids}" | while read pid; do
            kill -9 ${pid}
        done
    fi
}

function clean_env() {
    if [ -z "${1+x}" ]; then
        echo '[cmd clean_env] usage: <cmd> process-hint' >&2
		return 1
    fi
    local hint="${1}"
    kill_process pd "${hint}"
    kill_process tikv "${hint}"
    kill_process tidb "${hint}"
    kill_process tiflash "${hint}"
}

set -euo pipefail
clean_env "${@}"
