set -eu
source _env.sh
"$chbin" client --host "$chserver" --query "$@" -f PrettyCompactNoEscapes