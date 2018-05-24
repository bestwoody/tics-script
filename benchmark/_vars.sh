if [ `uname` == "Darwin" ]; then
	# *.dyso/*.a path for mac os
	gcc_path="`which gcc-7`"
	gcc_path="`readlink $gcc_path`"
	gcc_path="`dirname $gcc_path`"
	gcc_path="`dirname $gcc_path`"
	export DYLD_LIBRARY_PATH="$gcc_path/lib/gcc/7"
else
	# *.so/*.a path for linux
	export LD_LIBRARY_PATH="/usr/lib:/usr/local/lib:/usr/lib64:/usr/local/lib64"
fi
