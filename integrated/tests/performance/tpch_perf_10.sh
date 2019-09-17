#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/../_base/test_cases.sh"
auto_error_handle

tpch_perf "${BASH_SOURCE[0]}" 48 10
