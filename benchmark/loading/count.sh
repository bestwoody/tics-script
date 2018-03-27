selraw="$1"

set -eu

select="select"
if [ "$selraw" == "true" ]; then
	select="selraw"
fi

source _meta.sh

get_table_names | while read table; do
	echo $table:
	"$chbin" client --host="$chserver" --query="$select count() from $table"
done
