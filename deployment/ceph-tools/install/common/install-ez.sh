#!/bin/bash

set -eu

# TODO: what if version changed?
file="setuptools-33.1.1.zip"

rm -f "$file"

echo "=> curl https://bootstrap.pypa.io/ez_setup.py | sudo python"
curl https://bootstrap.pypa.io/ez_setup.py | sudo python

rm -f "$file"
