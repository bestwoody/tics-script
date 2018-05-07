set -eu

echo "=> unpack storage bin"
mkdir -p "./storage"
cp -f "./inventory/theflash" "./storage"
cp -f "./inventory/config.xml" "./storage/config.xml"
cp -f "./inventory/users.xml" "./storage/users.xml"

echo "=> install storage in current dir"

LD_LIBRARY_PATH=$LD_LIBRARY_PATH:`readlink -f storage`
function cp_required_lib
{
	ldd "./storage/theflash" | grep 'not found' | grep -v 'required by' | awk '{print $1}' | while read libfile; do
		libfile=`basename $libfile`
		if [ ! -f "./inventory/dylibs/$libfile" ]; then
			echo "=> $libfile required by theflash but not found in inventory/dylibs, exiting" >&2
			exit 1
		else
			cp -f "./inventory/dylibs/$libfile" "./storage"
		fi
	done
}
function cp_indirectly_required_lib
{
	cd "./storage"
	ldconfig
	ldd "./theflash" | grep 'not found' | grep 'required by' | awk -F ':' '{print $2}' | uniq | while read libfile; do
		libfile=`basename $libfile`
		if [ ! -f "../inventory/dylibs/$libfile" ]; then
			echo "=> $libfile required by theflash indirectly but not found in inventory/dylibs, exiting" >&2
			exit 1
		else
			cp -f "../inventory/dylibs/$libfile" "./"
		fi
	done
	cd ".."
}
echo "=> cp libs required by theflash"
# TODO: Need a loop here
cp_required_lib
cp_indirectly_required_lib
cp_required_lib
cp_indirectly_required_lib
cp_required_lib
cp_indirectly_required_lib

# TODO: Use supervisor or daemon mode, instead of launch script
echo "=> creating storage scripts"
echo "set -eu" > "./storage-server.sh"
echo "source ./_env.sh" >> "./storage-server.sh"
echo "cd storage" >> "./storage-server.sh"
echo "LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:\`readlink -f .\`" >> "./storage-server.sh"
cp -f "./storage-server.sh" "./storage-q.sh"
echo "./theflash server --config-file ./config.xml" >> "./storage-server.sh"
chmod +x "./storage-server.sh"
echo "./theflash client --host=\$chserver --query=\"create database if not exists \$chdb\"" >> "./storage-q.sh"
cp -f "./storage-q.sh" "./storage-cli.sh"
echo "./theflash client --host=\$chserver -d \$chdb --query \"\$@\"" >> "./storage-q.sh"
echo "./theflash client --host=\$chserver -d \$chdb" >> "./storage-cli.sh"
chmod +x "./storage-q.sh"
chmod +x "./storage-cli.sh"

echo "=> unpack scripts"
cp -f ./inventory/*.sh .
