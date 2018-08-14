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
	local jobs="$4"
	fio -filename="$file" -direct=1 -rw="$rw" -size 1000m -numjobs="$jobs" -bs="$bs" -runtime="$rt" -group_reporting -name=pc | \
		tee -a $log | grep IOPS | awk -F ': ' '{print $2}'
}

iops_w=`fio_test randwrite 4k 5 16 | awk -F ',' '{print $1}'`
iops_r=`fio_test randread 4k 5 16 | awk -F ',' '{print $1}'`
iotp_w=`fio_test randwrite 4m 5 16 | awk '{print $2}'`
iotp_r=`fio_test randread 4m 5 16 | awk '{print $2}'`

echo "Max: RandWrite: (4K)$iops_w (4M)$iotp_w, RandRead: (4K)$iops_r (4M)$iotp_r"

fio_stander()
{
	local threads="$1"
	wl_w=`fio_test write 64k 5 $threads`
	wl_r=`fio_test read 64k 5 $threads`
	wl_iops_w=`echo "$wl_w" | awk -F ',' '{print $1}'`
	wl_iops_r=`echo "$wl_r" | awk -F ',' '{print $1}'`
	wl_iotp_w=`echo "$wl_w" | awk '{print $2}'`
	wl_iotp_r=`echo "$wl_r" | awk '{print $2}'`
	echo "64K, $threads threads: Write: $wl_iops_w $wl_iotp_w, Read: $wl_iops_r $wl_iotp_r"
}
fio_stander " 8"
fio_stander "16"
fio_stander "32"

rm -f "$log.w.stable"
for ((i = 0; i < 5; i++)); do
	fio_test randwrite 64k 5 16 >> "$log.w.stable"
done
w_stable=(`cat "$log.w.stable" | awk -F 'BW=' '{print $2}' | awk '{print $1}'`)
echo "RandWrite stable test: ["${w_stable[@]}"]"

rm -f "$log.r.stable"
for ((i = 0; i < 5; i++)); do
	fio_test randread 64k 5 16 >> "$log.r.stable"
done
r_stable=(`cat "$log.r.stable" | awk -F 'BW=' '{print $2}' | awk '{print $1}'`)
echo "RandRead stable test: ["${r_stable[@]}"]"
