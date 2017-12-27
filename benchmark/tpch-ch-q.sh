n=$1

set -eu

if [ -z "$n" ]; then
	echo "usage: <bin> n"
	exit 1
fi

source _env.sh

sql=`cat sql-ch/q"$n".sql | tr '\n' ' '`
$chbin client --host 127.0.0.1 --query="$sql"
