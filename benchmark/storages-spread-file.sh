path="$1"
user="$2"

set -eu

source ./_env.sh

if [ -z "$path" ]; then
	echo "usage: <bin> path [user]" >&2
	exit 1
fi

if [ ! -z "$user" ]; then
	user="$user@"
fi

path=`readlink -f $path`

echo "=> target: $path"
for server in ${storage_server[@]}; do
	echo "=> copying to [$server]"
	scp $path ${user}${server}:${path}
done

