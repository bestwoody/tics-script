if [ "`uname`" == "Darwin" ] && [ -z "`which gcc-8`" ]; then
	echo "error: install gcc-8 first, to support feature 'variant' in c++17" >&2
	exit 1
fi

mkdir -p build && cd build && cmake .. && make
