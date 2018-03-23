set -eu

source _env.sh

"$chbin" client --host "$chserver" -d "$chdb" --query "$@"
