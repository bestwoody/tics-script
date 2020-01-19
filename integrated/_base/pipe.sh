#!/bin/bash

function uncolor()
{
	sed 's/\x1B\[[0-9;]\+[A-Za-z]//g'
}
export -f uncolor

function trim_host()
{
	python "${integrated}/_base/trim_host.py"
}
export -f trim_host

function scale_to_name()
{
	tr '.' '_'
}
export -f scale_to_name

function trim_space()
{
	sed -e 's/^[[:space:]]*//' | sed -e 's/[[:space:]]*$//'
}
export -f trim_space
