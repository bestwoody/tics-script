#!/bin/bash

function install_basic()
{
	local host="${1}"
	ssh ${host} "sudo yum install -y epel-release"
	ssh ${host} "systemctl stop firewalld"
	ssh ${host} "hostname ${host}"
	ssh ${host} "sudo yum install -y vim lsof telnet nc rsync net-tools.x86_64 dstat htop sysstat fio"
}

set -u
if [ -z "${1+x}" ]; then
	echo "usage: <bin> host" >&2
	exit 1
fi
install_basic "${@}" | awk '{print "['${1}'] "$0}'
