setup_gcc_on_mac()
{
	export CC=`which gcc-7`
	if [ -z "$CC" ]; then
		echo "gcc-7 not found, install it first, exiting" >&2
		exit
	fi
	export CXX=`which g++-7`
	if [ -z "$CXX" ]; then
		echo "g++-7 not found, install it first, exiting" >&2
		exit
	fi
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
