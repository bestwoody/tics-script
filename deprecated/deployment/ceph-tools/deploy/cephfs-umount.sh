#!/bin/bash

set -eu

if [ `whoami` != "root" ]; then
	echo "need 'sudo' to run umount, exiting" >&2
	exit 1
fi

path="$1"
if [ -z "$path" ]; then
	echo "usage: <bin> mounted-path"
	exit 1
fi

umount "$path"
