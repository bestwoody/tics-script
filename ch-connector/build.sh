target="$1"

set -eu

if [ -z "$target" ]; then
	target="theflash"
fi

source _build.sh
build_ch "ch" "$target"
