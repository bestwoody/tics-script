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

		if [ -z ${DYLD_LIBRARY_PATH+x} ]; then
			export DYLD_LIBRARY_PATH="$gcc_path/lib/gcc/7"
		else
			export DYLD_LIBRARY_PATH="$DYLD_LIBRARY_PATH:$gcc_path/lib/gcc/7"
		fi
	else
		# *.so/*.a path for linux
		local lib_path="/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib:`dirname $storage_bin`"
		if [ -z ${LD_LIBRARY_PATH+x} ]; then
			export LD_LIBRARY_PATH="$lib_path"
		else
			export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$lib_path"
		fi
	fi
}
export setup_dylib_path

print_spark_settings()
{
	echo 'spark.conf.set("spark.storage.plan.pushdown.agg", "'$pushdown'")'
	echo 'spark.conf.set("spark.storage.selraw", "'$selraw'")'
	echo 'spark.conf.set("spark.storage.partitionsPerSplit", "'$partitionsPerSplit'")'
	echo 'spark.conf.set("spark.flash.catalog_policy", "'flashfirst'")'
}
export print_spark_settings
