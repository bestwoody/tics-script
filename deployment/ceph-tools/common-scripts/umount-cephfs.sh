path="$1"
if [ -z "$path" ]; then
	echo "usage: <bin> mounted-path"
	exit 1
fi

sudo fusermount -u "$path"
