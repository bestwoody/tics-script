pattern="$1"
if [ -z "$pattern" ]; then
	pattern="multi_append"
fi

verb="$2"

python simulator.py oltp "$pattern" "conf/oltp.conf" "$verb"
