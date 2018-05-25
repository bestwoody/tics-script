source _meta.sh

get_table_names | while read table; do
	./drop.sh "$table"
done
