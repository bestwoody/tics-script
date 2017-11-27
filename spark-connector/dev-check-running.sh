master_check=`ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep | wc -l | awk '{print $1}'`
echo "$master_check master(s) is running"
slave_check=`ps -ef | grep org.apache.spark.deploy.worker.Worker | grep -v grep | wc -l | awk '{print $1}'`
echo "$slave_check slave(s) is running"
