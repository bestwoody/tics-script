master_check=`ps -ef | grep org.apache.spark.deploy.master.Master | grep -v grep | wc -l | awk '{print $1}'`
echo "$master_check master(s) is running"

slave_check=`ps -ef | grep org.apache.spark.deploy.worker.Worker | grep -v grep | wc -l | awk '{print $1}'`
slave_in_master_check=`curl -s http://127.0.0.1:8080 | grep ' Workers (' | awk -F '(' '{print $2}' | awk -F ')' '{print $1}'`
echo "$slave_check slave(s) is running"
echo "$slave_in_master_check slave(s) in master"
