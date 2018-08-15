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

echo "=> copying unified files from master:$from_path:"
confirm
cat ./upgrade-cp-conf-from.unified | while read file; do
	if [ -f "$from_path/$file" ]; then
		echo cp -f "$from_path/$file" "./$file"
		cp -f "$from_path/$file" "./$file"
		./storages-spread-file.sh "./$file"
	fi
done

echo "=> copying pernode files from master:$from_path:"
confirm
source ./_env.sh
cat ./upgrade-cp-conf-from.pernode | while read file; do
	abs_from="`realpath $from_path/$file`"
	abs_file="`realpath ./$file`"
	for server in ${storage_server[@]}; do
		host="`get_host $server`"
		if ssh "$host" "test -e $abs_from"; then
			echo scp "$host:$abs_from" "${abs_file}.${host}"
			scp "$host:$abs_from" "${abs_file}.${host}"
			echo scp "${abs_file}.${host}" "${host}:$abs_file"
			scp "${abs_file}.${host}" "${host}:$abs_file"
			rm -f "${abs_file}.${host}"
		fi
	done
done

echo "=> stopting all storage services:"
confirm
./storages-server-stop.sh

echo "=> starting all storage services:"
confirm
./storages-server-start.sh

echo "=> stopping spark master and workers:"
confirm
./storages-dsh.sh ./spark-stop-all.sh

echo "=> starting spark master:"
confirm
./spark-start-master.sh

echo "=> starting all spark workers:"
confirm
./storages-dsh.sh ./spark-start-slave.sh
sleep 2
./spark-check-running.sh

echo "=> done"
