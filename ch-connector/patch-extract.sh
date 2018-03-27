patch_ensure_not_changed()
{
	local target="$1"

	local patch=`git status $target-patch/*`
	local not_staged=`echo "$patch" | grep "Changes not staged"`
	local untracked=`echo "$patch" | grep "Untracked"`
	if [ ! -z "$not_staged" ] || [ ! -z "$untracked" ]; then
		echo "patch dir has modified content, 'git commit' or 'git checkout' it first, aborted" >&2
		exit 1
	fi
}

patch_cp()
{
	local target="$1"
	local modified="$2"

	local patch="../$target-patch/${modified#*$target}"
	local path=`dirname "$patch"`
	mkdir -p "$path"
	cp -f "$modified" "$patch"
}

patch_diff()
{
	local target="$1"
	local modified="$2"

	local patch="../$target-patch/${modified#*$target}"
	local path=`dirname "$patch"`
	mkdir -p "$path"

	if [ -f "$patch" ]; then
		cp -f "$modified" "$patch"
	else
		local delta=`git diff "$modified"`
		if [ ! -z "$delta" ]; then
			echo "$delta" > "$patch.patch"
		fi
	fi
}

untracked_extract() {
	local target="$1"
	local untracked="$2"

	if [ -d "$untracked" ]; then
		find "$untracked" -type f | while read file; do
			untracked_extract "$target" "$file"
		done
	else
		patch_cp "$target" "$untracked"
	fi
}

patch_extract_repo()
{
	local target="$1"

	cd "$target"
	git status | grep modified | awk -F 'modified:' '{print $2}' | while read modified; do
		patch_diff "$target" "$modified"
	done
	git status | grep 'Untracked files:' -A 99999 | grep "^\t" | while read untracked; do
		untracked_extract "$target" "$untracked"
	done
}

patch_extract_one()
{
	local target="$1"
	local modified="$2"
	cd "$target"
	patch_diff "$target" "$modified"
}

patch_extract()
{
	local target="$1"
	local path="$2"

	if [ -z "$path" ]; then
		patch_ensure_not_changed "$target"
		patch_extract_repo "$target"
	else
		patch_extract_one "$target" "$path"
	fi
}

extract_file="$1"
target="ch"

set -eu

patch_extract "$target" "$extract_file"
