#!/bin/bash

sql_file="$1"
output_file="$2"

if [[ -z "$sql_file" ]]; then
    sql_file="./sqls/check-batch-insert.sql"
fi

if [[ -z "$output_file" ]]; then
    output_file="/tmp/batch-insert-data"
fi

./run-sql-file.sh "${sql_file}" &&

cmd="select * from default.test FORMAT TabSeparatedRaw"
echo "${cmd} > ${output_file}"
./storage-client.sh "${cmd}" > ${output_file} &&

echo "start to check data"
./storage-client.sh "desc test" | python3 ./check-batch-insert.py ${output_file}
