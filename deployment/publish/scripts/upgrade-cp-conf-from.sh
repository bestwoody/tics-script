from_path="$1"

set -ue

if [ -z "$from_path" ]; then
	echo "usage: <bin> from-old-version-path(eg: ../theflash-78a0b2)" >&2
	exit 1
fi

confirm()
{
	read -p "=> hit enter to continue"
}

if [ ! -f "$from_path/package-info" ]; then
	echo "$from_path is not a valid package path" >&2
	exit 1
fi

if [ ! -f "./package-info" ]; then
	echo "current dir is not a valid package path" >&2
	exit 1
fi

old_info="`cat $from_path/package-info`"
new_info="`cat ./package-info`"

echo "=> copying config fiels from $from_path:"
echo "$old_info"
echo "=> to current dir:"
echo "$new_info"
confirm

cat ./upgrade-cp-conf-from.files | while read file; do
	if [ -f "$from_path/$file" ]; then
		echo cp -f "$from_path/$file" "./$file"
		cp -f "$from_path/$file" "./$file"
	fi
done

echo "=> copying config files to all storage nodes:"
./storages-dsh.sh ./storage-server-stop.sh
cat ./upgrade-cp-conf-from.files | while read file; do
	if [ -f "./$file" ]; then
		./storages-spread-file.sh "./$file"
	fi
done

echo "=> stopting all storage services:"
./storages-server-stop.sh

echo "=> starting all storage services:"
./storages-server-start.sh

echo "=> stopping spark master and workers:"
./storages-dsh.sh ./spark-stop-all.sh

echo "=> starting spark master:"
./spark-start-master.sh

echo "=> starting all spark workers:"
./storages-dsh.sh ./spark-start-slave.sh

echo "=> done"
