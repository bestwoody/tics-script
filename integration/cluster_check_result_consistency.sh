#!/bin/bash

set -eu
set -o pipefail

source ./_cluster_env.sh

target_db="$storage_db"

mysql_client="mysql -u root -P $tidb_port -h ${storage_server[0]} -e"

for table in `${mysql_client} "show tables from $target_db"`
do
    if [[ $table == *"Tables_in"* ]]; then
        continue
    fi
    sql="select count(*) from $target_db.$table"
    echo $sql
    while true; do
        spark_result=`get_spark_result "$sql" "$deploy_dir" "$user" "$spark_master"`
        echo "$spark_result"
        if [[ ! -z ${spark_result} ]]; then
            break
        fi
    done
    mysql_result=`get_mysql_result "$sql" "$tidb_port" "${storage_server[0]}"`
    echo "$mysql_result"
    if [[ "$spark_result" == "$mysql_result" ]]; then
       echo "result matching"
    else
       echo "result not matching. spark result $spark_result, tidb result $mysql_result"
       exit 1
    fi
done
