#!/bin/bash

set -eu

function execute_cmd()
{
	local host="$1"
	local cmd="$2"
	local deploy_dir="$3"
	local user="$4"
	local echo_host="false"
	if [[ $# -eq 5 ]];  then
	    echo_host="$5"
	fi
	cmd="cd ${deploy_dir}; $cmd"
    ssh "${user}@${host}" "$cmd" < /dev/null 2>&1 | while read line; do
        if [[ "$echo_host" == "true" ]]; then
            echo "[$host] $line"
        else
            echo "$line"
        fi
	done
}

export -f execute_cmd

function run_start_shell_on_host()
{
    local host=$1
    local cmd=$2
    local deploy_dir=$3
    local user=$4
    echo "$host $cmd"
    ssh "${user}@${host}" -q -t "set -m; cd ${deploy_dir}; ${cmd}" | while read line; do
        echo "$line"
    done
    echo "=> ${host} started"
}

export -f run_start_shell_on_host

function is_tiflash_running()
{
    local host="$1"
    local tiflash_bin_name="$2"
    local deploy_dir="$3"
    local user="$4"
    cmd="ps -ef | grep '${tiflash_bin_name} server' | grep -v grep | grep -v ssh | grep -v nohup | awk '{print \$2}'"
    pid=`execute_cmd "$host" "$cmd" "$deploy_dir" "$user" | awk '{print $1}'`
    if [ -z pid ]; then
        pid=0
    fi
    echo $pid
}

export -f is_tiflash_running

function is_tiflash_proxy_running()
{
    local host="$1"
    local tiflash_proxy_bin_name="$2"
    local deploy_dir="$3"
    local user="$4"
    cmd="ps -ef | grep '${tiflash_proxy_bin_name}' | grep -v grep | grep -v ssh | grep -v nohup | awk '{print \$2}'"
    pid=`execute_cmd "$host" "$cmd" "$deploy_dir" "$user" | awk '{print $1}'`
    if [ -z pid ]; then
        pid=0
    fi
    echo $pid
}

export -f is_tiflash_proxy_running

function port_ready()
{
    local server=$1
    local port=$2
    local deploy_dir=$3
    local user="$4"
    ssh "${user}@${server}" "cd ${deploy_dir}; lsof -i:${port} | wc -l" < /dev/null 2>&1 | while read line; do
        echo $line
    done
}

export -f port_ready

function stop_tiflash()
{
    local host="$1"
    local deploy_dir="$2"
    local user="$3"
    execute_cmd "$host" "./stop_rngine_and_tiflash.sh" "$deploy_dir" "$user" "true"
}

export -f stop_tiflash


function run_tiflash()
{
    local host="$1"
    local tiflash_tcp_port="$2"
    local deploy_dir="$3"
    local user="$4"
    run_start_shell_on_host "$host" "./run_tiflash.sh &" "${deploy_dir}" "${user}"
    sleep 30
    # check tiflash tcp port is ready
    ready="false"
    while [[ "$ready" != "true" ]]; do
        ready="true"
        if [[ `port_ready "$host" "$tiflash_tcp_port" "$deploy_dir" "$user"` -eq 0 ]]; then
            ready="false"
        fi
        echo "tiflash not ready. wait.."
        sleep 10
    done
    run_start_shell_on_host "$host" "./run_rngine.sh &" "${deploy_dir}" "${user}"
}

export -f run_tiflash

function restart_tiflash()
{
    local host="$1"
    local tiflash_tcp_port="$2"
    local deploy_dir="$3"
    local user="$4"
    stop_tiflash "$host" "$deploy_dir" "$user"
    sleep 5
    run_tiflash "$host" "$tiflash_tcp_port" "$deploy_dir" "$user"
}

export -f restart_tiflash

function need_deploy()
{
    local host="$1"
    local deploy_dir="$2"
    local user="$3"
    cmd="if [ -d ${deploy_dir} ]; then echo 0; else echo 1; fi"
    ssh "${user}@${host}" "$cmd" | while read line; do
        echo $line;
    done
}

export -f need_deploy

function get_learner_store_id()
{
    local server="$1"
    local deploy_dir="$2"
    local user="$3"
    local pd_port="$4"
    cmd="bin/pd-ctl -u http://${server}:${pd_port} <<< 'store'"
    result=""
    execute_cmd "$server" "$cmd" "$deploy_dir" "$user" |
    {
        while read line; do
            result="$result$line\n"
        done;
        echo -e $result | grep -B 10 "slave" | grep -B 2 $server | grep "id" | tr -cd "[0-9]"
    }
}

export -f get_learner_store_id

function get_normal_store_id()
{
    local server=$1
    local deploy_dir="$2"
    local user="$3"
    local pd_port="$4"
    cmd="bin/pd-ctl -u http://${server}:${pd_port} <<< 'store'"
    cmd="cd ${deploy_dir}; $cmd"
    result=""
    execute_cmd "$server" "$cmd" "$deploy_dir" "$user" |
    {
        while read line; do
            result="$result$line\n"
        done;
        echo -e $result | grep -B 10 "normal" | grep -B 2 $server | grep "id" | tr -cd "[0-9]"
    }
}

export -f get_normal_store_id

function delete_store()
{
    local server=$1
    local deploy_dir="$2"
    local user="$3"
    local pd_port="$4"
    echo "delete store"
    id=`get_learner_store_id "$server" "$deploy_dir" "$user" "$pd_port"`
    echo $id
    cmd="bin/pd-ctl -u http://${server}:${pd_port} <<< 'store delete ${id}'"
    execute_cmd "$server" "$cmd" "$deploy_dir" "$user"
}

export -f delete_store

function get_store_status()
{
    local server=$1
    local deploy_dir="$2"
    local user="$3"
    local pd_port="$4"

    id=`get_learner_store_id "$server" "$deploy_dir" "$user" "$pd_port"`
    if [[ -z $id ]]; then
        echo "Tomestone"
        return 0
    fi

    cmd="bin/pd-ctl -u http://${server}:${pd_port} <<< 'store ${id}'"
    execute_cmd "$server" "$cmd" "$deploy_dir" "$user" | while read line; do
        echo $line | grep "state_name" | awk '{print $2}' | tr -cd "[a-zA-Z]"
    done
}

export -f get_store_status

function get_store_region_count()
{
    local server="$1"
    local deploy_dir="$2"
    local user="$3"
    local pd_port="$4"
    local is_learner="$5"
    if [[ -z $is_learner ]]; then
        is_learner="false"
    fi
    if [[ is_learner == "true" ]]; then
        id=`get_learner_store_id "$server" "$deploy_dir" "$user" "$pd_port"`
    else
        id=`get_normal_store_id "$server" "$deploy_dir" "$user" "$pd_port"`
    fi

    if [[ -z $id ]]; then
        echo 0;
        return 0;
    fi
    cmd="bin/pd-ctl -u http://${server}:${pd_port} <<< 'store ${id}'"
    execute_cmd "$server" "$cmd" "$deploy_dir" "$user" | while read line; do
        echo $line | grep "region_count" | awk '{print $2}' | tr -cd "[0-9]"
    done
}

export -f get_store_region_count


function get_mysql_result()
{
    sql=$1
    tidb_port=$2
    host=$3
    if [[ -z "$sql" ]]; then
        return 0
    fi
    mysql_client="mysql -u root -P $tidb_port -h $host -e"
    mysql_result=`${mysql_client} "$sql"`
    mysql_result=`echo $mysql_result | tr -cd "[0-9]"`
    echo $mysql_result
}

export -f get_mysql_result

function get_spark_result()
{
    sql="$1"
    deploy_dir="$2"
    user="$3"
    host="$4"
    if [[ -z "$sql" ]]; then
        return 0
    fi
    spark_found="false"
    spark_cmd="cd ${deploy_dir}; ./spark_q.sh '$sql'"
    ssh "${user}@${host}" ${spark_cmd} < /dev/null 2>&1 | while read line; do
        if [[ "$spark_found" == "true" ]]; then
            spark_result=$line
            if [[ $spark_result =~ [0-9] ]]; then
                echo $spark_result | tr -cd "[0-9]"
                spark_found="false"
            fi
        fi
        if [[ $line == *"count(1)"* ]]; then
            spark_found="true"
        fi
    done
}

export -f get_spark_result
