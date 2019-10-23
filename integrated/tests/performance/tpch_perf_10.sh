#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/tpch_perf_base.sh"
auto_error_handle

tpch_perf "${BASH_SOURCE[0]}" 48 10
