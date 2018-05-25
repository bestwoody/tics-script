set -eu

source ./_env.sh

$storage_bin server --config-file "storage-server-config/config.xml"
