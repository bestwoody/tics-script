spark/sbin/stop-master.sh
spark/sbin/stop-slave.sh

sleep 0.5

master_check=`ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep | wc -l | awk '{print $1}'`
if [ "$master_check" != "0" ]; then
	echo "stop master failed: $master_check" >&2
fi
master_check=`ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep | wc -l | awk '{print $1}'`
if [ "$master_check" != "0" ]; then
	echo "stop master failed: $master_check" >&2
fi

echo "" >&2
echo "OK" >&2
