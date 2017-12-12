source _env.sh

import_blocks()
{
	local blocks="$1"
	local table="$2"

	for ((i = 0; i < $blocks; i++)); do
		let "n = $i + 1"
		local data="$dbgen_result_dir_prefix"$blocks"/$table.data.$n"
		if [ ! -f "$data" ]; then
			echo "$data: not found" >&2
			return 1
		fi
		cat "$data" | "$chbin" client --host="$chserver" --query="INSERT INTO $table FORMAT CSV" &
	done

	wait_sub_procs
}

import_block()
{
	local table="$1"

	local data="$dbgen_result_dir_prefix"$blocks"/$table.data"
	if [ ! -f "$data" ]; then
		echo "$data: not found" >&2
		return 1
	fi
	cat "$data" | "$chbin" client --host="$chserver" --query="INSERT INTO $table FORMAT CSV"
}

import_table()
{
	local blocks="$1"
	local table="$2"

	local file="$dbgen_result_dir_prefix"$blocks"/$table.data"
	if [ -f "$file" ] || [ "$blocks" == "1" ]; then
		import_block "$table"
	else
		import_blocks "$blocks" "$table"
	fi
}
export -f import_table
