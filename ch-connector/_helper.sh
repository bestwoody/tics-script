setup_gcc_on_mac()
{
	export CC=`which gcc-7`
	if [ -z "$CC" ]; then
		export CC=`which gcc-6`
		if [ -z "$CC" ]; then
			echo "gcc-7/6 not found, install it first, exiting" >&2
			exit
		fi
	fi
	export CXX=`which g++-7`
	if [ -z "$CXX" ]; then
		export CXX=`which g++-6`
		if [ -z "$CXX" ]; then
			echo "g++-7 not found, install it first, exiting" >&2
			exit
		fi
	fi

	local sys_include="/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk/usr/include"

	export CPLUS_INCLUDE_PATH=`$CXX -x c++ -v -E /dev/null 2>&1 | \
		grep '<...> search starts here' -A 99 | \
		grep 'End of search list' -B 99 | \
		grep -v ' search ' | awk '{print $1}' | tr '\n' ':' | sed 's/:$//'`
	export CPLUS_INCLUDE_PATH="$CPLUS_INCLUDE_PATH:/usr/local/include:$sys_include"
	echo "CXX include: $CPLUS_INCLUDE_PATH"

	export C_INCLUDE_PATH=`$CXX -x c++ -v -E /dev/null 2>&1 | \
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
