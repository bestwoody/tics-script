#!/bin/bash

FLASH_HOME="$(dirname `cd $(dirname ${BASH_SOURCE[0]}) && pwd`)"
cd $FLASH_HOME/storage

target="$1"
type="$2"

set -eu

if [ -z "$target" ]; then
	target="tiflash"
fi

if [ -z "$type" ]; then
	type="RELWITHDEBINFO"
fi


cd $FLASH_HOME/storage/ch/contrib/kvproto
./generate_cpp.sh

cd $FLASH_HOME/storage/ch/contrib/tipb
./generate-cpp.sh

cd $FLASH_HOME/storage
source ./_build.sh
build_ch "ch" "$target" "false" "$type"
