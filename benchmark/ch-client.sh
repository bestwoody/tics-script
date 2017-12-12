set -eu

source _env.sh

$chbin client --query "$@"
