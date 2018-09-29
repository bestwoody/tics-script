#!/bin/bash

ignore="$1"
in="$2"
out="$3"

set -eu

if [ -z "$ignore" ]; then 
	ignore="./syslibs.list"
fi
if [ -z "$in" ]; then 
	in="./yumlibs.list"
fi
if [ -z "$out" ]; then 
	out="./downloaded/yumlibs/libs.list"
fi

rm -f "$out"

add_to_deps()
{
	local lib="$1"
	echo "$lib" >> "$out"
	local subs=`sudo yum deplist $lib 2>&1 | python parse-yum-deps.py "$out" "$ignore"`
	if [ ! -z "$subs" ]; then
		echo "=> parsing $lib deplist"
		echo "$subs" | while read sub; do
			add_to_deps "$sub"
		done
		echo "=> $lib OK"
	fi
}

while read lib; do
	add_to_deps "$lib"
done < "$in"
