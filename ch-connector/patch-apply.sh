origin_ensure_not_changed()
{
	local target="$1"
	local old=`pwd`

	cd "$target"

	local patch=`git status`
	local not_staged=`echo "$patch" | grep "Changes not staged"`
	local untracked=`echo "$patch" | grep "Untracked"`
	if [ ! -z "$not_staged" ] || [ ! -z "$untracked" ]; then
		echo "origin dir has modified content, remove it first, aborted" >&2
		exit 1
	fi

	cd "$old"
}

patch_apply()
{
	local target="$1"

	find "$target-patch" -type f | while read patch_file; do
		local origin_file="$target""${patch_file#*patch}"
		local origin_path=`dirname "$origin_file"`
		local patch_ext="${patch_file##*.}"

		mkdir -p "$origin_path"

		if [ "$patch_ext" == "patch" ]; then
			origin_file="${origin_file%.patch}"
			if [ ! -f "$origin_file" ]; then
				echo "origin file $origin_file missed, aborted" >&2
			fi
			# echo "patching '$origin_file' with '$patch_file'"
			patch -p0 "$origin_file" < "$patch_file"
		else
			echo "cp '$origin_file'"
			cp "$patch_file" "$origin_file"
		fi
		echo "OK"
	done
}

set -eu

target="ch"
origin_ensure_not_changed "$target"
patch_apply "$target"
