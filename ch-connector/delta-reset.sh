untracked_reset()
{
	local untracked="$1"

	if [ -d "$untracked" ]; then
		find "$untracked" -type f | while read file; do
			untracked_reset "$file"
		done
	else
		rm "$untracked"
	fi
}


delta_reset()
{
	git checkout delta/*
	git status | grep 'Untracked files:' -A 99999 | grep "^\t" | while read untracked; do
		untracked_reset "$untracked"
	done
}

set -eu
delta_reset
