patch_diff()
{
	local target="$1"
	local modified="$2"

	local patch="../$target-patch/${modified#*$target}"
	local path=`dirname "$patch"`
	mkdir -p "$path"
	git diff "$modified" > "$patch.patch"
}

patch_extract()
{
	local target="$1"
	local modified="$2"
	cd "$target"
	patch_diff "$target" "$modified"
}

modified="$1"

set -eu

if [ -z "$modified" ]; then
	echo "usage: <bin> modified-file-path-in-repo" >&2
fi

target="clickhouse"
patch_extract "$target" "$modified"
