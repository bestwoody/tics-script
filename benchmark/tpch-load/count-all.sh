selraw="$1"

source _meta.sh

get_table_names | while read table; do
	./count.sh "$table" "$selraw"
	if [ ${#storage_server[@]} -gt 1 ]; then
		echo
	fi
done
