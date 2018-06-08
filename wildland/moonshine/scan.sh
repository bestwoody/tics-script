table="$1"

set -eu

if [ -z "$table" ]; then
	table="test"
fi

source ./_env.sh

build/moonshine scan "$path" "$table"
