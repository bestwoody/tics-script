ip=`ifconfig | grep -i mask | grep broadcast | grep inet | awk '{print $2}'`

# TODO: check ip

spark/sbin/start-master.sh
spark/sbin/start-slave.sh $ip:7077

# TODO: check alive
