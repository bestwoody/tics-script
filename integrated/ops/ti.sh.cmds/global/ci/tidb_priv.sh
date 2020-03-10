#!/bin/bash

function cmd_ti_ci_tidb_privilege()
{
  local ti="${integrated}/ops/ti.sh"

  local dir='/tmp/ti/ci/tidb_privilege'
  mkdir -p "${dir}"
  local file="${dir}/tidb_privilege.ti"
  rm -f "${file}"

  "${ti}" new "${file}" 'delta=-20' "dir=${dir}" 1>/dev/null
  "${ti}" "${file}" must burn
  "${ti}" "${file}" run
  "${ti}" "${file}" ci/tidb_priv
  "${ti}" "${file}" must burn

  rm -f "${file}"
  print_hhr
  echo 'ci/tidb_privilege OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_ci_tidb_privilege
