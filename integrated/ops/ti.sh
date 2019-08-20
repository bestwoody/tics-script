#!/bin/bash
source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
source ${integrated}/_base/cmd_ti.sh
cmd_ti -s "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/ti.sh.cmds" "${@}"
