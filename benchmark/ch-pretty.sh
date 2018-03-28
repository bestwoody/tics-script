set -eu
source _env.sh

"$chbin" client --host="$chserver" --query="create database if not exists $chdb"
if [ $? != 0 ]; then
	echo "create database '"$chdb"' failed" >&2
	exit 1
fi

"$chbin" client --host "$chserver" -d "$chdb" --query "$@" -f PrettyCompactNoEscapes
