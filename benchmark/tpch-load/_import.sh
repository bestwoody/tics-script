source ./_env.sh

print_cat_data()
{
	local data="$1"
	local mycat="cat $data"
	if [ ! -f "$data" ]; then
		mycat="gzip -dc $data.gz"
	fi
	echo "$mycat"
}
export print_cat_data

ensure_table_created()
{
	local host="$1"
	local port="$2"
	local table="$3"

	"$storage_bin" client --host="$host" --port="$port" --query="create database if not exists $storage_db"

	local schema="$meta_dir/schema/$table.schema"
	if [ ! -f "$schema" ]; then
		echo "$schema schema file not found" >&2
		return 1
	fi

	"$storage_bin" client --host="$host" --port="$port" -d "$storage_db" --query="`cat $schema`"
}
export -f ensure_table_created

ensure_tables_created()
{
	local table="$1"

	for server in ${storage_server[@]}; do
		ensure_table_created "`get_host $server`" "`get_port $server`" "$table"
	done
}
export -f ensure_tables_created

import_blocks()
{
	local blocks="$1"
	local table="$2"

	if [ $(( $tpch_blocks % ${#storage_server[@]} )) -ne 0 ]; then
		echo "servers % blocks != 0, exiting" >&2
		return 1
	fi
	local blocks_per_server=$(( $tpch_blocks / ${#storage_server[@]} ))

	for ((i = 0; i < $blocks; i++)); do
		local n=$(( $i + 1 ))
		local data="$dbgen_result_dir_prefix"$blocks"/$table.data.$n"
		if [ ! -f "$data" ] && [ ! -f "$data.gz" ]; then
			echo "$data/$data.gz: not found" >&2
			return 1
		fi

		local server_index=$(( $i / $blocks_per_server ))
		local server=${storage_server[$server_index]}
		local host="`get_host $server`"
		local port="`get_port $server`"
		local cat_data="`print_cat_data $data`"

		ensure_table_created "$host" "$port" "$table"
		$cat_data | "$storage_bin" client --host="$host" --port="$port" \
			-d "$storage_db" --query="INSERT INTO $table FORMAT CSV" &
	done

	wait_sub_procs
}

import_block()
{
	local table="$1"

	local data="$dbgen_result_dir_prefix"$blocks"/$table.data"
	if [ ! -f "$data" ] && [ ! -f "$data.gz" ]; then
		echo "$data[.gz]: not found" >&2
		return 1
	fi
	ensure_tables_created "$table"
	local server=${storage_server[0]}
	local cat_data="`print_cat_data $data`"
	$cat_data | "$storage_bin" client --host="`get_host $server`" --port="`get_port $server`" \
		-d "$storage_db" --query="INSERT INTO $table FORMAT CSV"
}

import_table()
{
	local blocks="$1"
	local table="$2"

	local file="$dbgen_result_dir_prefix"$blocks"/$table.data"
	if [ -f "$file" ] || [ -f "$file.gz" ] || [ "$blocks" == "1" ]; then
		import_block "$table"
	else
		import_blocks "$blocks" "$table"
	fi
}
export -f import_table
