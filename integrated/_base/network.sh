#!/bin/bash

function print_ip()
{
	ifconfig | grep 'en[0123456789]\|eth[0123456789]\|wlp[0123456789]\|em[0123456789]' -A 3 | \
		grep -v inet6 | grep inet | grep -i mask | awk '{print $2}'
}
export -f print_ip

function print_ip_cnt()
{
	local ips="`print_ip`"
	if [ -z "${ips}" ]; then
		echo "0"
	else
		echo "${ips}" | wc -l | awk '{print $1}'
	fi
}
export -f print_ip_cnt

function must_print_ip()
{
	local ip_cnt="`print_ip_cnt`"
	if [ "${ip_cnt}" != "1" ]; then
		echo "127.0.0.1"
	else
		print_ip
	fi
}
export -f must_print_ip

function cal_addr()
{
	if [ -z ${1+x} ]; then
		echo "[func cal_addr] usage: <func> addr default_host default_port [default_pd_name]" >&2
		return 1
	fi

	local addr="${1}"
	local default_host="${2}"
	local default_port="${3}"
	local default_pd_name=""
	if [ ! -z "${4+x}" ]; then
		local default_pd_name="${4}"
	fi
	python "${integrated}/_base/cal_addr.py" "${addr}" "${default_host}" "${default_port}" "${default_pd_name}"
}
export -f cal_addr
