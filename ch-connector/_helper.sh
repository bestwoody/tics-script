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
