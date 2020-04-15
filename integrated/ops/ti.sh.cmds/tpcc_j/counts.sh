set -eu

if [ -z "${2+x}" ]; then
	echo "usage: <bin> cluster scale" >&2
	exit 1
fi

cluster="${1}"
scale="${2}"
db="tpcc_${scale}"
db="test"

tables=(
	bmsql_config
	bmsql_customer
	bmsql_district
	bmsql_history
	bmsql_item
	bmsql_new_order
	bmsql_oorder
	bmsql_order_line
	bmsql_stock
	bmsql_warehouse
)
tables=(
	customer
	district
	history
	item
	new_order
	orders
	order_line
	stock
	warehouse
)
tables=(
	new_order
	orders
	order_line
)
for table in ${tables[@]}; do
	echo "=> ${table}"
	ti.sh ${cluster}.ti mysql "select count(*) from ${db}.${table}" test
done
