set -eu
source ./_env.sh
cd storage
LD_LIBRARY_PATH=`readlink -f .`:$LD_LIBRARY_PATH
./theflash server --config-file ./config.xml
