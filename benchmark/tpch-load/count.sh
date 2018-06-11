table="$1"
selraw="$2"

set -eu

if [ -z "$table" ]; then
	echo "usage: <bin> table-name [selraw=false]" >&2
	exit 1
fi

select="select"
if [ "$selraw" == "true" ]; then
	select="selraw"
fi

source _env.sh

if [ $(( $tpch_blocks % ${#storage_server[@]} )) -ne 0 ]; then
	echo "servers % blocks != 0, exiting" >&2
	exit 1
fi

echo [$table]
sum=0
for server in ${storage_server[@]}; do
	count=$("$storage_bin" client --host=`get_host $server` --port=`get_port $server` \
		-d "$storage_db" --query="$select count() from $table")
	echo "$server: $count"
	sum=$(( $sum + $count ))
done

if [ ${#storage_server[@]} -gt 1 ]; then
	echo "total: $sum"
fi
