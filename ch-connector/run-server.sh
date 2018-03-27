set -eu
source _env.sh
mkdir -p "running/theflash/db"
"$chbin" server --config-file "running/config/config.xml"
