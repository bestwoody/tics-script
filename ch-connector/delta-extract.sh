delta_ensure_not_changed()
{
	local delta=`git status delta`
	local not_staged=`echo "$delta" | grep "Changes not staged"`
	local untracked=`echo "$delta" | grep "Untracked"`
	if [ ! -z "$not_staged" ] || [ ! -z "$untracked" ]; then
		echo "delta dir has modified content, commit or remove it first, aborted" >&2
		exit 1
	fi
}

delta_cp()
{
	local target="$1"
	local modified="$2"

	local delta="../delta/${modified#*$target}"
	local path=`dirname "$delta"`
	mkdir -p "$path"
	cp "$modified" "$delta"
}

delta_extract()
{
	local target="$1"

	cd "$target"
	git status | grep modified | awk -F 'modified:' '{print $2}' | while read modified; do
		delta_cp "$target" "$modified"
	done
	git status | grep 'Untracked files:' -A 99999 | grep "^\t" | while read untracked; do
		delta_cp "$target" "$untracked"
	done
}

set -eu
delta_ensure_not_changed
delta_extract "clickhouse"
