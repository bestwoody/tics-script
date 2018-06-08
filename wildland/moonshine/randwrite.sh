set -eu

source ./_env.sh

block_rows="2"
total_rows="2"

build/moonshine randwrite "$path" "$schema" "$block_rows" "$total_rows"
