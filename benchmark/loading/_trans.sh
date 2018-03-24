source _env.sh

trans_blocks()
{
	local blocks="$1"
	local table="$2"

	for ((i = 0; i < $blocks; i++)); do
		let "n = $i + 1"
		local file="$dbgen_result_dir_prefix"$blocks"/$table.tbl.$n"
		local data="$dbgen_result_dir_prefix"$blocks"/$table.data.$n"
		if [ ! -f "$file" ]; then
			echo "$file: not found" >&2
			return 1
		fi
		if [ -f "$data" ]; then
			echo "$data: exists, skipped transform" >&2
			continue
		fi
		cat "$file" | python "$meta_dir/trans/$table.py" > "$data" &
	done

	wait_sub_procs
}

trans_block()
{
	local table="$1"

	local file="$dbgen_result_dir_prefix"$blocks"/$table.tbl"
	local data="$dbgen_result_dir_prefix"$blocks"/$table.data"
	if [ ! -f "$file" ]; then
		echo "$file: not found" >&2
		return 1
	fi
	if [ -f "$data" ]; then
		echo "$data: exists, skipped transform" >&2
		return 0
	fi
	cat "$file" | python "$meta_dir/trans/$table.py" > "$data"
}

trans_table()
{
	local blocks="$1"
	local table="$2"

	local file="$dbgen_result_dir_prefix"$blocks"/$table.tbl"
	if [ -f "$file" ] || [ "$blocks" == "1" ]; then
		trans_block "$table"
	else
		trans_blocks "$blocks" "$table"
	fi
}
export -f trans_table
