#!/bin/bash

function uncolor()
{
	sed 's/\x1B\[[0-9;]\+[A-Za-z]//g'
}
export -f uncolor

function trim_host()
{
	local here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
	python "${here}/trim_host.py"
}
export -f trim_host
