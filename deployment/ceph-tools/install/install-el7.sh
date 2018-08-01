#!/bin/bash

set -eu

cd el7
echo "=> el7> ./install.sh"
./install.sh

cd ..
cd common

echo "=> common> ./install-ez.sh"
./install-ez.sh

echo "=> common> ./install-itsdangerous.sh"
./install-itsdangerous.sh
