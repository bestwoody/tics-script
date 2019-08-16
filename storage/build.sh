#!/bin/bash

target="$1"
type="$2"

set -eu

if [ -z "$target" ]; then
	target="theflash"
fi

if [ -z "$type" ]; then
	type="RELWITHDEBINFO"
fi

source ./_build.sh
echo "`cd ch/contrib/kvproto && ./generate_cpp.sh`"

build_ch "ch" "$target" "false" "$type"
