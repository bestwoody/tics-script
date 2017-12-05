set -eu

update_submodule()
{
	local path="$1"
	local old=`pwd`
	cd "$path"
	echo `pwd`"> git submodule update"
	git submodule update --init
	cd "$old"
}

update_submodules()
{
	cat .gitmodules | grep 'submodule' | awk -F '\"' '{print $2}' | while read line; do
		update_submodule "$line"
	done
}

cd "clickhouse"
update_submodules
