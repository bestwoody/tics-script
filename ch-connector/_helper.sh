setup_gcc_on_mac()
{
	export DYLD_FALLBACK_LIBRARY_PATH="/usr/local/lib:/usr/lib"

	export CC=`which gcc-6`
	if [ -z "$CC" ]; then
		echo "gcc-6 not found, install it first, exiting" >&2
		exit
	fi
	export CXX=`which g++-6`
	if [ -z "$CXX" ]; then
		echo "g++-6 not found, install it first, exiting" >&2
		exit
	fi

	local gcc_path=`realpath $CXX`
	gcc_path=`dirname $gcc_path`
	gcc_path=`dirname $gcc_path`
	gcc_path=`find "$gcc_path" -name stdlib.h | head -n 1`
	gcc_path=`dirname "$gcc_path"`
	if [ -z "$gcc_path" ]; then
		echo "gcc include path not found, exiting" >&2
		exit 1
	fi
	export CPLUS_INCLUDE_PATH="$gcc_path":/usr/local/include:/usr/include:$CPLUS_INCLUDE_PATH
	export C_INCLUDE_PATH="$CPLUS_INCLUDE_PATH"

	echo "CC:"
	echo "  $CC"
	echo "CXX:"
	echo "  $CXX"
	echo "C_INCLUDE_PATH and CPLUS_INCLUDE_PATH:"
	echo "  $CPLUS_INCLUDE_PATH"
	echo "DYLD_FALLBACK_LIBRARY_PATH:"
	echo "  $DYLD_FALLBACK_LIBRARY_PATH"
}
export -f setup_gcc_on_mac

setup_gcc_on_linux()
{
	export CC="gcc"
	export CXX="g++"
}
export -f setup_gcc_on_linux

setup_gcc()
{
	if [ "`uname`" == "Darwin" ]; then
		setup_gcc_on_mac
	else
		setup_gcc_on_linux
	fi
}
export -f setup_gcc
