set -eu

repo_reset()
{
	local target="$1"

	cd "$target"
	git status | grep modified | awk -F 'modified:' '{print $2}' | xargs git checkout
	# Keep the untracked for safty
}

repo_reset "clickhouse"
