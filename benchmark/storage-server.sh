set -eu
source ./_env.sh
$storage_bin server --config-file "$storage_server_config"
