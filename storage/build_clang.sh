#!/bin/bash

FLASH_HOME="$(dirname `cd $(dirname ${BASH_SOURCE[0]}) && pwd`)"
cd $FLASH_HOME/storage

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
elif [ -z "$type" ]; then
	build_type="Debug"
else
	build_type="$type"
fi

cd $FLASH_HOME/storage/ch/contrib/kvproto
./generate_cpp.sh

cd $FLASH_HOME/storage
source ./_build.sh
build_ch "ch" "tiflash" "true" "$build_type"
