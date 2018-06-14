source ./_env.sh

ip="$1"
if [ -z "$ip" ] && [ ! -z "$spark_master" ]; then
	ip="$spark_master"
fi

set -eu

./spark-stop-all.sh

if [ -z "$ip" ]; then
	if [ `uname` == "Darwin" ]; then
		ip=`ifconfig | grep -i mask | grep broadcast | grep inet | awk '{print $2}'`
	else
		if [ ! -z `which ip` ]; then
			ip=`ip addr | grep brd | grep scope | awk '{print $2}' | awk -F '/' '{print $1}'`
		else
			ip=`ifconfig | grep -i mask | grep cast | grep inet | awk '{print $2}' | awk -F 'addr:' '{print $2}'`
		fi
	fi
fi

if [ -z "$ip" ]; then
	ip="127.0.0.1"
fi

echo "local ip: $ip"

ip_check="`echo $ip | wc -l | awk '{print $1}'`"
if [ "$ip_check" != "1" ]; then
	echo "get local ip failed: $ip_check" >&2
	exit 1
fi

./spark-start-master.sh $ip
./spark-start-slave.sh $ip

echo
echo "OK"

sleep 0.5

./spark-check-running.sh
