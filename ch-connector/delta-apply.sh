delta_apply()
{
	local target="$1"
	find "delta" -type f | while read delta_file; do
		local origin_file="$target""${delta_file#*delta}"
		if [ -f "$origin_file" ]; then
			local backup_file="origin${delta_file#*delta}"
			local backup_path=`dirname "$backup_file"`
			mkdir -p "$backup_path"
			cp "$origin_file" "$backup_file"
		fi
		cp "$delta_file" "$origin_file"
	done
}

set -eu
delta_apply "clickhouse"
