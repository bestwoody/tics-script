#!/bin/bash

set -eu

old=`pwd`

cd ../../offline/downloaded/
sudo python ez_setup.py

cd "$old"
