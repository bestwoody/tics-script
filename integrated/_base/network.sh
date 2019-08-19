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