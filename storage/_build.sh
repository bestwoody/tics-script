#!/bin/bash

setup_gcc_on_mac()
{
	export CC=`which gcc-7`
	if [ -z "$CC" ]; then
		echo "gcc-7 not found, install it first, exiting" >&2
		exit 1
	fi
	export CXX=`which g++-7`
	if [ -z "$CXX" ]; then
		echo "g++-7 not found, install it first, exiting" >&2
		exit 1
	fi

	local sys_include="/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk/usr/include"

	export CPLUS_INCLUDE_PATH=`$CXX -x c++ -v -E /dev/null 2>&1 | \
		grep '<...> search starts here' -A 99 | \
		grep 'End of search list' -B 99 | \
		grep -v ' search ' | awk '{print $1}' | tr '\n' ':' | sed 's/:$//'`
	export CPLUS_INCLUDE_PATH="$CPLUS_INCLUDE_PATH:/usr/local/include:$sys_include"
	echo "CXX include: $CPLUS_INCLUDE_PATH"

	export C_INCLUDE_PATH=`$CC -x c++ -v -E /dev/null 2>&1 | \
		grep '<...> search starts here' -A 99 | \
		grep 'End of search list' -B 99 | \
		grep -v ' search ' | awk '{print $1}' | tr '\n' ':' | sed 's/:$//'`
	export C_INCLUDE_PATH="$C_INCLUDE_PATH:/usr/local/include:$sys_include"
	echo "C include: $C_INCLUDE_PATH"
}
export -f setup_gcc_on_mac

setup_gcc_on_linux()
{
	export CC="gcc"
	export CXX="g++"
	local gcc_major=`gcc -v 2>&1 | grep '^gcc' | awk '{print $3}' | awk -F '.' '{print $1}'`
	local gxx_major=`g++ -v 2>&1 | grep '^gcc' | awk '{print $3}' | awk -F '.' '{print $1}'`
	if [ "$gcc_major" != "7" ]; then
		echo "gcc found but not gcc-7, install it first, exiting" >&2
		exit 1
	fi
	if [ "$gxx_major" != "7" ]; then
		echo "g++ found but not g++-7, install it first, exiting" >&2
		exit 1
	fi
}
export -f setup_gcc_on_linux

setup_gcc()
{
	if [ "`uname`" == "Darwin" ]; then
		setup_gcc_on_mac
	else
		setup_gcc_on_linux
	fi
	export LIBRARY_PATH="/usr/lib:/usr/local/lib:/usr/lib64:/usr/local/lib64"
}
export -f setup_gcc


setup_clang()
{
	if [ -z "${CC+x}" ]; then
		if [ "`uname`" == "Darwin" ] && [ -f /usr/local/opt/llvm/bin/clang ]; then
			export CC=/usr/local/opt/llvm/bin/clang
		else
			if [ "`uname`" == "Darwin" ]; then
				echo "Warning: The default Apple LLVM is likely not going to work, at least for 9.0.0 or older. Consider install an official LLVM clang."
			fi
			export CC=`which clang`
		fi
	fi
	if [ -z "${CXX+x}" ]; then
		if [ "`uname`" == "Darwin" ] && [ -f /usr/local/opt/llvm/bin/clang++ ]; then
			export CXX=/usr/local/opt/llvm/bin/clang++
		else
			if [ "`uname`" == "Darwin" ]; then
				echo "Warning: The default Apple LLVM is likely not going to work, at least for 9.0.0 or older. Consider install an official LLVM clang."
			fi
			export CXX=`which clang++`
		fi
	fi

	if [ -z "$CC" ]; then
		echo "Please setup clang 5 or higher version."
		if [ "`uname`" == "Darwin" ]; then
			echo "For MacOSX, brew install --with-toolchain llvm && export PATH=\"/usr/local/opt/llvm/bin:$PATH\""
		fi
		exit 1
	fi

	if [ -z "$CXX" ]; then
		echo "Please setup clang 5 or higher version."
		if [ "`uname`" == "Darwin" ]; then
			echo "For MacOSX, brew install --with-toolchain llvm && export PATH=\"/usr/local/opt/llvm/bin:$PATH\""
		fi
		exit 1
	fi
}

make_ch()
{
	local build_dir="$1"
	local source_related_dir="$2"
	local build_target="$3"
	local is_clang="$4"
	local build_type="$5"

	if [ "$is_clang" = "true" ]; then
		setup_clang
	else
		setup_gcc
	fi

	mkdir -p "$build_dir"
	local old=`pwd`
	cd "$build_dir"

	# https://stackoverflow.com/a/23378780
	physical_cpu_count=$([ $(uname) = 'Darwin' ] && sysctl -n hw.physicalcpu_max || lscpu -p | egrep -v '^#' | sort -u -t, -k 2,4 | wc -l)

	# physical_cpu_count=1

	if [ "$is_clang" = "true" ]; then
		if [ "$build_type" = "ASan" ]; then
			cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=$CXX -DCMAKE_C_COMPILER=$CC -DCMAKE_BUILD_TYPE=ASan -DENABLE_TCMALLOC=0
		else
			cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=$CXX -DCMAKE_C_COMPILER=$CC -DCMAKE_BUILD_TYPE=$build_type
		fi
	else
		cmake "$source_related_dir" -DCMAKE_CXX_COMPILER=$CXX -DCMAKE_C_COMPILER=$CC -DCMAKE_BUILD_TYPE=$build_type -Wno-dev
	fi

	make -j $physical_cpu_count "$build_target"
	cd "$old"
}
export -f make_ch

build_ch()
{
	local target_dir="$1"
	local build_target="$2"
	local is_clang="$3"
	local build_type="$4"

	if [ "$is_clang" = "true" ]; then
		build_dir="build_clang"
	else
		build_dir="build"
	fi

	mkdir -p "$build_dir"
	make_ch "$build_dir" "../$target_dir" "$build_target" "$is_clang" "$build_type"
}
export -f build_ch
