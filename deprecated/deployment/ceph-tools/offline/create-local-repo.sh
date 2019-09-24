#!/bin/bash

dir="$1"

set -eu

if [ -z "$dir" ]; then
	dir="./downloaded/yumlibs"
fi

install_pkg()
{
	local dir="$1"
	local pkg="$2"
	local file="$dir/`ls "$dir" | grep "$pkg" | head -n 1`"
	if [ -z $file ]; then
		echo "=> $pkg not found, exiting"
		exit 1
	fi

	local pkg_installed=`rpm -qa | grep "$pkg"`
	if [ ! -z "$pkg_installed" ]; then
		echo "=> $pkg installed, skipped"
	else
		sudo rpm -i "$file"
	fi
}

install_pkg "$dir" "deltarpm"
install_pkg "$dir" "python-deltarpm"
install_pkg "$dir" "createrepo"

createrepo `readlink -f "$dir"`
