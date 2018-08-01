#!/bin/bash

file="$1"

set -eu

if [ -z "$file" ]; then
	echo "usage: <bin> test-file-path" >&2
	exit 1
fi

log="$file.log"
rm -f "$log"

fio_test()
{
	local rw="$1"
	local bs="$2"
	local rt="$3"
	fio -filename="$file" -direct=1 -rw="$rw" -size 1000m -numjobs=16 -bs="$bs" -runtime="$rt" -group_reporting -name=pc | \
		tee -a $log | grep IOPS | awk -F ': ' '{print $2}'
}

iops_w=`fio_test randwrite 4k 5 | awk -F ',' '{print $1}'`
iops_r=`fio_test randread 4k 5 | awk -F ',' '{print $1}'`
iotp_w=`fio_test randwrite 2m 5 | awk '{print $2}'`
iotp_r=`fio_test randread 2m 5 | awk '{print $2}'`

echo "Max: RandWrite: $iops_w $iotp_w, RandRead: $iops_r $iotp_r"

wl_w=`fio_test randwrite 64k 5`
wl_r=`fio_test randread 64k 5`
wl_iops_w=`echo "$wl_w" | awk -F ',' '{print $1}'`
wl_iops_r=`echo "$wl_r" | awk -F ',' '{print $1}'`
wl_iotp_w=`echo "$wl_w" | awk '{print $2}'`
wl_iotp_r=`echo "$wl_r" | awk '{print $2}'`

echo "64K: Write: $wl_iops_w $wl_iotp_w, Read: $wl_iops_r $wl_iotp_r"

rm -f "$log.w.stable"
for ((i = 0; i < 5; i++)); do
	fio_test randwrite 64k 5 >> "$log.w.stable"
done
w_stable=(`cat "$log.w.stable" | awk -F 'BW=' '{print $2}' | awk '{print $1}'`)
echo "RandWrite detail: ["${w_stable[@]}"]"

rm -f "$log.r.stable"
for ((i = 0; i < 5; i++)); do
	fio_test randread 64k 5 >> "$log.r.stable"
done
r_stable=(`cat "$log.r.stable" | awk -F 'BW=' '{print $2}' | awk '{print $1}'`)
echo "RandRead detail: ["${r_stable[@]}"]"
