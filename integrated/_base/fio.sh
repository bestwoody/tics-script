function _fio_test()
{
	local rw="${1}"
	local bs="${2}"
	local rt="${3}"
	local jobs="${4}"
	local file="${5}"
	local log="${6}"

	fio -filename="${file}" -direct=1 -rw="${rw}" -size 1000m -numjobs="${jobs}" -bs="${bs}" -runtime="${rt}" -group_reporting -name=pc | \
		tee -a ${log} | { grep IOPS || test $? == 1; } | awk -F ': ' '{print $2}'
}
export -f _fio_test

function _fio_standard()
{
	local threads="$1"
	local file="${2}"
	local log="${3}"

	local wl_w=`_fio_test 'write' 64k 5 "${threads}" "${file}" "${log}"`
	local wl_r=`_fio_test 'read' 64k 5 "${threads}" "${file}" "${log}"`
	local wl_iops_w=`echo "${wl_w}" | awk -F ',' '{print $1}'`
	local wl_iops_r=`echo "${wl_r}" | awk -F ',' '{print $1}'`
	local wl_iotp_w=`echo "${wl_w}" | awk '{print $2}'`
	local wl_iotp_r=`echo "${wl_r}" | awk '{print $2}'`
	echo "64K, ${threads} threads: Write: ${wl_iops_w} ${wl_iotp_w}, Read: ${wl_iops_r} ${wl_iotp_r}"
}
export -f _fio_standard

function fio_report()
{
	iops_w=`_fio_test randwrite 4k 5 16 "${file}" "${log}" | awk -F ',' '{print $1}'`
	iops_r=`_fio_test randread 4k 5 16 "${file}" "${log}" | awk -F ',' '{print $1}'`
	iotp_w=`_fio_test randwrite 4m 5 16 "${file}" "${log}" | awk '{print $2}'`
	iotp_r=`_fio_test randread 4m 5 16 "${file}" "${log}" | awk '{print $2}'`

	echo "Max: RandWrite: (4K)${iops_w} (4M)${iotp_w}, RandRead: (4K)${iops_r} (4M)${iotp_r}"

	_fio_standard " 8" "${file}" "${log}"
	_fio_standard "16" "${file}" "${log}"
	_fio_standard "32" "${file}" "${log}"

	rm -f "$log.w.stable"
	for ((i = 0; i < 5; i++)); do
		_fio_test randwrite 64k 5 16 "${file}" "${log}" >> "$log.w.stable"
	done
	w_stable=(`cat "$log.w.stable" | awk -F 'BW=' '{print $2}' | awk '{print $1}'`)
	echo "RandWrite stable test: ["${w_stable[@]}"]"

	rm -f "$log.r.stable"
	for ((i = 0; i < 5; i++)); do
		_fio_test randread 64k 5 16 "${file}" "${log}" >> "$log.r.stable"
	done
	r_stable=(`cat "$log.r.stable" | awk -F 'BW=' '{print $2}' | awk '{print $1}'`)
	echo "RandRead stable test: ["${r_stable[@]}"]"
}
export -f fio_report
