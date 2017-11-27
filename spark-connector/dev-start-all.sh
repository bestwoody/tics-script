ip=`ifconfig | grep -i mask | grep broadcast | grep inet | awk '{print $2}'`
ip_check="`echo $ip | wc -l | awk '{print $1}'`"
if [ "$ip_check" != "1" ]; then
	echo "get local ip failed: $ip_check" >&2
	exit 1
fi

spark/sbin/start-master.sh
master_check=`ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep | wc -l | awk '{print $1}'`
if [ "$master_check" != "1" ]; then
	echo "launch master failed: $master_check" >&2
	exit 1
fi

spark/sbin/start-slave.sh $ip:7077
slave_check=`ps -ef | grep org.apache.spark.deploy.worker.Worker | grep -v grep | wc -l | awk '{print $1}'`
if [ "$slave_check" != "1" ]; then
	echo "launch slave failed: $slave_check" >&2
	exit 1
fi

echo "" >&2
echo "OK" >&2
