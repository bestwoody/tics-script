#!/bin/bash
source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
source ${integrated}/_base/cmd_kp.sh
cmd_kp "${@}"
