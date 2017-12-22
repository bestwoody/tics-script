n=$1

set -eu

if [ -z "$n" ]; then
	echo "usage: <bin> n"
	exit 1
fi

source _env.sh

sql=`cat sql-ch/q"$n".sql | tr '\n' ' '`
$chbin client --query="$sql"
