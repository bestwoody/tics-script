set -eu

source _env.sh

$chbin client --host 127.0.0.1 --query "$@"
