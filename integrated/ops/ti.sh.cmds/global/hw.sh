#!/bin/bash

function cmd_ti_perf_hw()
{
	auto_error_handle
	hw_info
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
cmd_ti_perf_hw "${@}"
