#!/bin/bash

set -eu

old=`pwd`

cd ../../download/downloaded/
sudo python ez_setup.py

cd "$old"
