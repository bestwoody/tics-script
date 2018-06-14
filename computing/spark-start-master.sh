source ./_env.sh

ip="$1"
if [ -z "$ip" ] && [ ! -z "$spark_master" ]; then
	ip="$spark_master"
fi

set -eu

spark/sbin/start-master.sh --host $ip
master_check=`ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep | wc -l | awk '{print $1}'`
if [ "$master_check" != "1" ]; then
	echo "launch master failed: $master_check" >&2
	exit 1
fi
