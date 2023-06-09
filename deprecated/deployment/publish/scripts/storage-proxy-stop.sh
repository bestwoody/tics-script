set -eu

pids=`./storage-proxy-pid.sh`

if [ -z "$pids" ]; then
	echo "tiflash-proxy pid not found, skipped and exiting"
	exit
fi

pids_count=`echo "$pids" | wc -l | awk '{print $1}'`

if [ "$pids_count" != "1" ]; then
	echo "found $pids_count tiflash-proxy process, closing them..."
fi

heavy_kill="false"
heaviest_kill="false"

echo "$pids" | while read pid; do
	for ((i=0; i<600; i++)); do
		if [ "$heaviest_kill" == "true" ]; then
			echo "#$i pid $pid closing, using 'kill -9'..."
			kill -9 $pid
		else
			if [ "$heavy_kill" == "true" ]; then
				echo "#$i pid $pid closing, using double kill..."
			else
				echo "#$i pid $pid closing..."
			fi
			kill $pid
			if [ "$heavy_kill" == "true" ]; then
				kill $pid
			fi
		fi

		sleep 1

		pid_exists=`ps -ef | grep theflash | grep "$pid"`
		if [ -z "$pids_exists" ]; then
			echo "#$i pid $pid closed"
			break
		fi

		if [ $i -ge 29 ]; then
			heavy_kill="true"
		fi
		if [ $i -ge 59 ]; then
			heaviest_kill="true"
		fi
		if [ $i -ge 119 ]; then
			echo "pid $pid close failed" >&2
			exit 1
		fi
	done
done
