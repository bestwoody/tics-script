make_ch()
{
	local build_dir="$1"
	local source_related_dir="$2"
	local build_target="$3"

	mkdir -p "$build_dir"
	local old=`pwd`
	cd "$build_dir"

	local CC=`which gcc-6`
	if [ -z "$CC" ]; then
		CC="gcc"
	fi
	local CXX=`which g++-6`
	if [ -z "$CXX" ]; then
		CXX="g++"
	fi

	echo cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=$CXX -DCMAKE_C_COMPILER=$CC
	cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=$CXX -DCMAKE_C_COMPILER=$CC
	make -j 4 "$build_target"
	cd "$old"
}

build_ch()
{
	local target_dir="$1"
	local build_target="$2"

	mkdir -p "build"
	make_ch "build" "../$target_dir" "$build_target"
}
export -f build_ch
