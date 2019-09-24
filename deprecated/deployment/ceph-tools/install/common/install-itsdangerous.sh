#!/bin/bash

set -eu

itsdangerous_installed=`rpm -qa | grep itsdangerous`

if [ ! -z "$itsdangerous_installed" ]; then
	echo "=> python-itsdangerous installed, skipped"
else
	old=`pwd`
	cd ../../offline/downloaded
	file="python-itsdangerous-0.23-2.el7.noarch.rpm"
	echo "=> sudo rpm -i $file"
	sudo rpm -i "$file"
	cd "$old"
fi
