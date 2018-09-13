#!/bin/bash

dir="$1"

set -eu

if [ -z "$dir" ]; then
	dir="downloaded"
fi

files=(
"https://mirrors.tuna.tsinghua.edu.cn/epel/epel-release-latest-7.noarch.rpm"
"http://mirror.centos.org/centos/7/extras/x86_64/Packages/python-itsdangerous-0.23-2.el7.noarch.rpm"
"https://bootstrap.pypa.io/ez_setup.py"
"https://pypi.io/packages/source/s/setuptools/setuptools-33.1.1.zip"
"https://download.ceph.com/keys/release.asc"
)

mkdir -p "$dir"

for file in ${files[@]}; do
	basename=`basename $file`
	if [ -f "$dir/$basename" ]; then
		echo "=> $basename ($file) exists, ignore"
		continue
	fi

	echo "=> downloading $file"
	curl -L "$file" > "$dir/${basename}.tmp"
	mv -f "$dir/${basename}.tmp" "$dir/$basename"
done
