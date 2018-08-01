#!/bin/bash

type="$1"

set -eu

if [ "$type" = "-h" ]; then
	echo "build_clang.sh [option]"
	echo " option:"
	echo " <empty> - Debug build type;"
	echo " -a      - ASan build type;"
	exit 0
elif [ "$type" = "-a" ]; then
	build_type="ASan"
else
	build_type="Debug"
fi

source ./_build.sh
build_ch "ch" "theflash" "true" "$build_type"
