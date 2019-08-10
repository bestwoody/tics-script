#!/bin/bash

arg="$1"

set -eu
set -o pipefail

source ./_cluster_env.sh

./cluster_mysql_client.sh "create database if not exists ${storage_db}"
./cluster_mysql_client.sh "create table if not exists ${storage_db}.simple(a int, b varchar(200))"
./cluster_mysql_client.sh "insert into ${storage_db}.simple values(1, 'hello world')"
