source ./_meta.sh

dbgen_bin()
{
	$dbgen_dir/dbgen -f -b $dbgen_dir/dists.dss $@ 2>/dev/null
}
export -f dbgen_bin

get_dbgen_flag()
{
	local table="$1"
	cat "$meta_dir/tables" | grep "$table$" | awk -F '\t' '{print $1}'
}
export -f get_dbgen_flag

dbgen_all_in_one()
{
	local dir="$dbgen_result_dir_prefix"1

	if [ -d "$dir" ]; then
		echo "$dir: exists, skipped dbgen" >&2
		return 0
	fi

	local old=`pwd`
	mkdir -p "$dir"
	cd "$dir"
	dbgen_bin -s "$tpch_scale"
	cd "$old"
}
export -f dbgen_all_in_one

dbgen_table_in_one()
{
	local table="$1"
	local dir="$dbgen_result_dir_prefix"1
	local flag=`get_dbgen_flag $table`

	if [ -f "$dir/$table.tbl" ]; then
		echo "$dir/$table.tbl: exists, skipped dbgen" >&2
		return 0
	fi
	if [ -f "$dir/$table.data" ] || [ -f "$dir/$table.data.gz" ]; then
		echo "$dir/$table.data[.gz]: exists, skipped dbgen" >&2
		return 0
	fi

	if [ -z "$flag" ]; then
		echo "unknown table name: $table" >&2
		return 1
	fi

	local old=`pwd`
	mkdir -p "$dir"
	cd "$dir"
	dbgen_bin -s "$tpch_scale" $flag
	cd "$old"
}
export -f dbgen_table_in_one

dbgen_table_blocks()
{
	local blocks="$1"
	local table="$2"
	local dir="$dbgen_result_dir_prefix""$blocks"

	if [ -f "$dir/$table.tbl" ]; then
		echo "$dir/$table.tbl: exists, skipped dbgen" >&2
		return 0
	fi
	if [ -f "$dir/$table.data" ] || [ -f "$dir/$table.data.gz" ]; then
		echo "$dir/$table.data[.gz]: exists, skipped dbgen" >&2
		return 0
	fi
	if [ -f "$dir/$table.tbl.1" ]; then
		echo "$dir/$table.tbl.*: exists, skipped dbgen" >&2
		return 0
	fi
	if [ -f "$dir/$table.data.1" ] || [ -f "$dir/$table.data.1.gz" ]; then
		echo "$dir/$table.data.*[.gz]: exists, skipped dbgen" >&2
		return 0
	fi

	local flag=`get_dbgen_flag $table`
	if [ -z "$flag" ]; then
		echo "unknown table name: $table" >&2
		return 1
	fi

	local old=`pwd`
	mkdir -p "$dir"
	cd "$dir"

	for ((i = 0; i < $blocks; i++)); do
		let "n = $i + 1"
		dbgen_bin -s "$tpch_scale" -C "$blocks" -S "$n" $flag &
	done

	wait_sub_procs
	cd "$old"
}
export -f dbgen_table_blocks

dbgen_tables_blocks()
{
	local blocks="$1"
	get_table_names | while read table; do
		dbgen_table_blocks "$blocks" "$table"
	done
}
export -f dbgen_tables_blocks

dbgen()
{
	local blocks="$1"
	local table="$2"

	if [ ! -f $dbgen_dir/dbgen ]; then
		echo "dbgen not found, exiting" >&2
		return 1
	fi

	if [ "$blocks" == "1" ]; then
		if [ -z "$table" ]; then
			dbgen_all_in_one
		else
			dbgen_table_in_one $table
		fi
	else
		if [ -z "$table" ]; then
			dbgen_tables_blocks "$blocks"
		else
			dbgen_table_blocks "$blocks" "$table"
		fi
	fi
}
export -f dbgen
