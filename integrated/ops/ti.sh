#!/bin/bash
source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/conf.sh"
cmd_ti -s "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/ti.sh.cmds" "${@}"
