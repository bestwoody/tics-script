#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/../_base/test_cases.sh"
auto_error_handle

tidb_tpcc_perf "${BASH_SOURCE[0]}" 33 1 1
