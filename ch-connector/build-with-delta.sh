update_submodule()
{
	local path="$1"
	local old=`pwd`
	cd "$path"
	git submodule update
	cd "$old"
}

update_submodules()
{
	cat .gitmodules | grep 'submodule' | awk -F '\"' '{print $2}' | while read line; do
		update_submodule "$line"
	done
}

make_ch()
{
	local build_dir="$1"
	local source_related_dir="$2"
	local target="$3"

	mkdir -p "$build_dir"
	local old=`pwd`
	cd "$build_dir"
	echo cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=`which g++-6` -DCMAKE_C_COMPILER=`which gcc-6`
	cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=`which g++-6` -DCMAKE_C_COMPILER=`which gcc-6`
	make -j `sysctl -n hw.ncpu` "$target"
	cd "$old"
}

merge_delta()
{
	mkdir -p "origin"
	mkdir -p "delta"
	find "delta" -type f | while read delta_file; do
		local origin_file="clickhouse${delta_file#*delta}"
		local backup_file="origin${delta_file#*delta}"
		local backup_path=`dirname $backup_file`
		mkdir -p "$backup_path"
		cp "$origin_file" "$backup_file"
		cp "$delta_file" "$origin_file"
	done
}

main()
{
	local target="$1"

	merge_delta

	cd "clickhouse"
	update_submodules
	cd ".."

	mkdir -p "build"
	make_ch "build" "../clickhouse" "$target"
}

full="$1"
set -eu
main "clickhouse"
