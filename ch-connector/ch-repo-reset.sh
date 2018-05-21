force="$1"

set -eu

if [ "$force" != "-f" ]; then
	echo "patch-apply.sh don't need ch-repo-reset.sh anymore! if you really want to reset ch repo, add '-f' arg."
	exit 1
fi

source _reset.sh

cd "ch"
repo_reset
