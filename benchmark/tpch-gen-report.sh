#!/bin/bash

file="$1"

default="./tpch.log"

if [ -z "$file" ]; then
	if [ -f "$default" ]; then
		file="$default"
	else
		echo "usage: <bin> tpch-log-file(default: tpch.log)"
		exit 1
	fi
fi

cat "$file" | python tpch-gen-report.py
