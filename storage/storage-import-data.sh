#!/bin/bash

name="$1"

set -eu

source ./_env.sh

if [ -z "$name" ]; then
	echo "usage: <bin> data-name(in running/data)" >&2
	exit 1
fi

gen=""
data="running/data/$name.data"
if [ -f "$data" ]; then
	gen="cat $data"
fi

if [ -z "$gen" ]; then
	gen="running/data/$name.sh"
	if [ ! -f "$gen" ]; then
		gen=""
	else
		gen="bash $gen"
	fi
fi

if [ -z "$gen" ]; then
	gen="running/data/$name.py"
	if [ ! -f "$gen" ]; then
		gen=""
	else
		gen="python $gen"
	fi
fi

if [ -z "$gen" ]; then
	echo "no data found, exit" >&2
	exit 1
fi

schema="running/data/$name.schema"
if [ -f "$schema" ]; then
	"$storage_bin" client --host 127.0.0.1 --query="`cat $schema`"
fi

$gen | "$storage_bin" client --host 127.0.0.1 --query="INSERT INTO $name FORMAT CSV"
