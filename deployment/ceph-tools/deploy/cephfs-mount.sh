#!/bin/bash

path="$1"
ceph_path="$2"

set -eu

if [ `whoami` != "root" ]; then
	echo "need 'sudo' to run mount, exiting" >&2
	exit 1
fi

if [ -z "$ceph_path" ]; then
	echo "usage: <bin> mount-to-path cephfs-path" >&2
	exit 1
fi

keyfile="/etc/ceph/ceph.client.admin.keyring"
if [ ! -f "$keyfile" ]; then
	echo "key file not found: $keyfile" >&2
	exit 1
fi

key=`cat $keyfile | grep key | awk -F '= ' '{print $2}'`

host=`ceph -s | grep 'mds: ' | grep '1 up ' | awk '{print $4}' | awk -F '=' '{print $2}'`
if [ -z "$host" ]; then
	echo "can't fetch active mds address from 'ceph -s'"
	exit 1
fi

mount -t ceph $host:6789:$ceph_path "$path" -o name=admin,secret=$key
