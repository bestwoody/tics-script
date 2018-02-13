delta_apply()
{
	local target="$1"
	find "$target-delta" -type f | while read delta_file; do
		local origin_file="$target""${delta_file#*delta}"
		local origin_path=`dirname "$origin_file"`
		mkdir -p "$origin_path"
		cp "$delta_file" "$origin_file"
	done
}

set -eu
delta_apply "clickhouse"
