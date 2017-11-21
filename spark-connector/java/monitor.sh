set -ef

pid=`ps -ef | grep java | grep MagicProto | grep -v grep | awk '{print $2}'`

if [ -z "$pid" ]; then
	echo "no java cli of magic protocol found, exiting" >&2
	exit 1
fi

top -pid $pid
