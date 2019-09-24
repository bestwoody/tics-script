#!/bin/bash

host="$1"
osd_img="$2"
osd_mb="$3"
dev_name="$4"

set -ue

if [ -z "$dev_name" ]; then
	echo "usage: <bin> host"
	exit 1
fi

disk_py="/usr/lib/python2.7/dist-packages/ceph_volume/util/disk.py"
if [ ! -f "$disk_py" ]; then
	disk_py="/usr/lib/python2.7/site-packages/ceph_volume/util/disk.py"
	if [ ! -f "$disk_py" ]; then
		echo "can't find disk.py, for checking loop device supporting" >&2
		echo "exiting now" >&2
	fi
fi

lo_supported=`ssh $host grep loop $disk_py`
if [ -z "$lo_supported" ]; then
	echo "loop device not supported, make sure 'disk.py' are edited to support it." >&2
	echo "refer: https://github.com/ceph/ceph/pull/21289" >&2
	echo "exiting now" >&2
	exit 1
fi

echo ssh $host "dd if=/dev/zero of=$osd_img count=$osd_mb bs=1M"
ssh $host "dd if=/dev/zero of=$osd_img count=$osd_mb bs=1M"
echo ssh $host "mkfs.ext4 -y $osd_img"
ssh $host "mkfs.ext4 -F $osd_img"
echo ssh $host "sudo chown ceph:ceph $osd_img"
ssh $host "sudo chown ceph:ceph $osd_img"
echo ssh $host "sudo losetup $dev_name $osd_img"
ssh $host "sudo losetup $dev_name $osd_img"
