path="$1"
if [ -z "$path" ]; then
	echo "usage: <bin> mounted-path"
	exit 1
fi

if [ `whoami` != "root" ]
	echo "need 'sudo' to run umount, exiting" >&2
	exit 1
fi

umount "$path"
