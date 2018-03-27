set -ef

pid=`ps -ef | grep java | grep TheFlashProto | grep -v grep | awk '{print $2}'`

if [ -z "$pid" ]; then
	echo "no java cli of TheFlashProto found, exiting" >&2
	exit 1
fi

top -pid $pid
