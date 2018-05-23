set -eu
source ./_env.sh
cd storage
LD_LIBRARY_PATH=`readlink -f .`:$LD_LIBRARY_PATH
./theflash client --host=$chserver --query="create database if not exists $chdb"
./theflash client --host=$chserver -d $chdb --query "$@"
