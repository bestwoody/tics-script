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
	git status --porcelain | grep '^ M ' | while read modified; do
		local modified="${modified:2}"
		git checkout "$modified"
	done

	git status --porcelain | grep '^\?? ' | while read untracked; do
		local untracked="${untracked:3}"
		_untracked_reset "$untracked"
	done
}
export -f repo_reset
