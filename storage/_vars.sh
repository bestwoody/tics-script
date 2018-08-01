#!/bin/bash

setup_dylib_path()
{
	if [ `uname` == "Darwin" ]; then
		# *.dyso/*.a path for mac os
		local gcc_name="gcc-7"
		local gcc_path="`which $gcc_name`"
		gcc_path="`readlink $gcc_path`"
		gcc_path="`dirname $gcc_path`"
		gcc_path="`dirname $gcc_path`"
		export DYLD_LIBRARY_PATH="$gcc_path/lib/gcc/7"
	else
		# *.so/*.a path for linux
		export LD_LIBRARY_PATH="`dirname $storage_bin`:/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib"
	fi
}
export setup_dylib_path
