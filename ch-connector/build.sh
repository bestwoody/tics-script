make_ch()
{
	local build_dir="$1"
	local source_related_dir="$2"
	local build_target="$3"

	mkdir -p "$build_dir"
	local old=`pwd`
	cd "$build_dir"
	echo cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=`which g++-6` -DCMAKE_C_COMPILER=`which gcc-6`
	cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=`which g++-6` -DCMAKE_C_COMPILER=`which gcc-6`
	make -j `sysctl -n hw.ncpu` "$build_target"
	cd "$old"
}

main()
{
	local target="$1"
	local build_target="$2"

	mkdir -p "build"
	make_ch "build" "../$target" "$build_target"
}

set -eu
main "clickhouse" "ch"
