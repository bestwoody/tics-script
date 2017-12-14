set -eu

top_cmd="top -p"
if [ `uname` == "Darwin" ]; then
	top_cmd="top -pid"
fi

pid=`ps -ef | grep /usr/bin/java |grep -v grep | awk '{print $2}'`

if [ ! -z "$pid" ]; then
	$top_cmd $pid
fi
