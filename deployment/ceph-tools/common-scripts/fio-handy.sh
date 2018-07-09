file="$1"

set -eu

if [ -z "$file" ]; then
	echo "usage: <bin> test-file-path" >&2
	exit 1
fi

fio -filename="$file" -direct=1 -rw=write -size 1000m -numjobs=16 -bs=64k -runtime=5 -group_reporting -name=pc
fio -filename="$file" -direct=1 -rw=read -size 1000m -numjobs=16 -bs=64k -runtime=5 -group_reporting -name=pc
fio -filename="$file" -direct=1 -rw=randwrite -size 1000m -numjobs=16 -bs=1m -runtime=5 -group_reporting -name=pc
fio -filename="$file" -direct=1 -rw=randread -size 1000m -numjobs=16 -bs=1m -runtime=5 -group_reporting -name=pc
