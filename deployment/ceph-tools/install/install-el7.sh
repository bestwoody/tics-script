#!/bin/bash

repo_url="$1"

set -eu

if [ -z "$repo_url" ]; then
	repo_url="file://`readlink -f ../offline/downloaded/yumlibs`"
fi

cd el7
echo "=> el7> ./install.sh $repo_url"
./install.sh "$repo_url"

cd ..
cd common

echo "=> common> ./install-ez.sh"
./install-ez.sh

echo "=> common> ./install-itsdangerous.sh"
./install-itsdangerous.sh
