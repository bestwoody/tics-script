set -eu

bench()
{
	local pattern="$1"
	echo "$pattern"

	count=`grep count conf/default.conf | awk '{print $NF}'`

	for f in conf/tests/*; do
		res=`python simulator.py write_then_scan "$pattern" $f 0 | grep -i 'DB read' | awk '{print $(NF-1)}'`
		echo "$res" | while read line; do
			if [ "$line" == "$count" ]; then
				echo "  Passed: $f"
			else
				echo "  Failed: $f, output $line, should be $count"
				break
			fi
		done
	done
}

bench append
bench reversed_append
bench uniform
bench multi_append
bench hotzone
bench hotkeys
