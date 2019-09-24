#!/bin/bash

db="$1"
table="$2"

set +e

source ./_env.sh

if [ -z "$db" ] || [ -z "$table" ]; then
	echo "usage: <bin> db-name table-name">&2
	exit 1
fi

ret=0

for server in ${storage_server[@]}; do
  echo [$server]
  "$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" \
    --query="DBGInvoke refresh_schema($db, $table)"
  tmp=$?
  if [ $tmp != 0 ] && [ $tmp != 60 ]; then # 0 is success and 60 is unkonwn table, ignore them.
    echo "Refresh schema on node $server failed with error code $tmp">&2
    ret=$tmp
  fi
  echo
done

if [ $ret == 0 ]; then
  echo "Refresh schema SUCCESS!"
fi

exit $ret

