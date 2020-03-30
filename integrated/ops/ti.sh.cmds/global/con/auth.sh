#!/bin/bash

function setup_auth()
{
	local host="${1}"
	local key=`cat ~/.ssh/id_rsa.pub`
	ssh ${host} "mkdir -p .ssh && echo \"${key}\" > ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
}

set -u
if [ -z "${1+x}" ]; then
	echo "usage: <bin> host" >&2
	exit 1
fi
setup_auth "${@}"
