#!/bin/bash

ignore="$1"
in="$2"
out="$3"
dir="$4"

if [ -z "$ignore" ]; then 
	ignore="./syslibs.list"
fi
if [ -z "$in" ]; then 
	in="./yumlibs.list"
fi
if [ -z "$dir" ]; then 
	dir="./downloaded/yumlibs"
fi
if [ -z "$out" ]; then 
	out="$dir/libs.list"
fi

echo "=> installing yum-plugin-downloadonly"
sudo yum install yum-plugin-downloadonly >/dev/null

echo "=> parsing yumlibs deps"
./parse-yum-deps.sh "$ignore" "$in" "$out"

cat "$out" | uniq | while read lib; do
	echo "=> downloading $lib with 'yum reinstall'"
	sudo yum reinstall --setopt=protected_multilib=false --downloadonly --downloaddir="$dir" $lib
	echo "=> downloading $lib done"
	if [ $? != 0 ]; then
		echo "=> downloading $lib with 'yum install'"
		sudo yum install --setopt=protected_multilib=false --downloadonly --downloaddir="$dir" $lib
		if [ $? != 0 ]; then
			echo "=> $lib failed" >&2
			echo "$lib" >> failed.list
			continue
		fi
	fi
done
