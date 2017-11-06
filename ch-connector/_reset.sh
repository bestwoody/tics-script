_untracked_reset()
{
	local untracked="$1"

	if [ -d "$untracked" ]; then
		find "$untracked" -type f | while read file; do
			_untracked_reset "$file"
		done
	else
		rm "$untracked"
	fi
}

repo_reset()
{
	local target="$1"

	#git status "$target" | grep modified | awk -F 'modified:' '{print $2}' | xargs git checkout
	git checkout "$target"

	git status "$target" | grep 'Untracked files:' -A 99999 | grep "^\t" | while read untracked; do
		_untracked_reset "$untracked"
	done
}
export -f repo_reset
