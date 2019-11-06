#!/bin/bash

function timer_start()
{
	if [ `uname` == "Darwin" ]; then
		date +%s
	else
		date +%s%N
	fi
}
export -f timer_start

function timer_end()
{
	if [ -z "${1+x}" ]; then
		echo "[func timer_end] usage: <func> start_time" >&2
		return 1
	fi

	local start_time="${1}"
	if [ `uname` == "Darwin" ]; then
		local end_time=`date +%s`
		local elapsed=$((end_time - start_time))
		echo "${elapsed}s"
	else
		local end_time=`date +%s%N`
		local elapsed=$(( (end_time - start_time) / 1000000 ))
		echo "${elapsed}ms"
	fi
}
export -f timer_end
