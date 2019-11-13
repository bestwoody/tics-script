#!/bin/bash

function cmd_ti_perf_hw()
{
	hw_info
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_perf_hw "${@}"
