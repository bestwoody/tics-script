source _env.sh

ip="$spark_master"

if [ -z "$ip" ]; then
	if [ `uname` == "Darwin" ]; then
		ip=`ifconfig | grep -i mask | grep broadcast | grep inet | awk '{print $2}'`
	else
		ip=`ifconfig | grep -i mask | grep cast | grep inet | awk '{print $2}' | awk -F 'addr:' '{print $2}'`
	fi
fi

if [ -z "$ip" ]; then
	ip="127.0.0.1"
fi

echo "master ip: $ip"

spark/bin/spark-shell --master spark://$ip:7077 --executor-memory $spark_executor_memory $@ 2>&1
