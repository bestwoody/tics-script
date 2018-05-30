file="$1"

set -eu

if [ -z "$file" ]; then
	file="/data/fio-test-file"
fi

fio -filename="$file" -direct=1 -rw=write -size 500m -numjobs=16 -bs=1m -runtime=5 -group_reporting -name=pc
