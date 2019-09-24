#!/bin/bash

source ./_cluster_env.sh

target_db=$storage_db

completed="false"
mysql_client="mysql -u root -P $tidb_port -h ${storage_server[0]} -e"

while [[ $completed != "true" ]]; do
    sleep 120
    echo "check whether data load is ready"
    completed="true"
    for table in `${mysql_client} "show tables from $target_db"`
    do
        if [[ "$table" == *"Tables_in"* ]]; then
            continue
        fi
        sql="select count(*) from $target_db.$table"
        mysql_result1=`get_mysql_result "$sql" "$tidb_port" "${storage_server[0]}"`
        sleep 30
        mysql_result2=`get_mysql_result "$sql" "$tidb_port" "${storage_server[0]}"`
        if [[ ${mysql_result1} != ${mysql_result2} ]]; then
            echo "mysql result"
            echo $mysql_result1
            echo $mysql_result2
            completed="false"
            break
        fi

        spark_result1=`get_spark_result "$sql" "$deploy_dir" "$user" "$spark_master"`
        sleep 10
        spark_result2=`get_spark_result "$sql" "$deploy_dir" "$user" "$spark_master"`
        if [[ -z $spark_result1 ]]; then
            completed="false"
            break
        fi
        if [[ ${spark_result1} != ${spark_result2} ]]; then
            echo "spark result"
            echo $spark_result1
            echo $spark_result2
            completed="false"
            break
        fi
    done
done