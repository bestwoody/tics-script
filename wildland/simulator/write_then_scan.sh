pattern="$1"
if [ -z "$pattern" ]; then
	pattern="multi_append"
fi

verb="$2"

python simulator.py write_then_scan "$pattern" "conf/write_then_scan.conf" "$verb"
