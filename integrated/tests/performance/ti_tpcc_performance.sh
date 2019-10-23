#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/ti_tpcc_performance_base.sh"
auto_error_handle

tidb_tpcc_perf "${BASH_SOURCE[0]}" 33 1 1