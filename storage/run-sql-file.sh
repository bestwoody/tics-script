#!/bin/bash

file="$1"
color="$2"

if [ -z "$color" ]; then
	color="true"
fi

source ./_run.sh

function exe()
{
	while read line; do
		if [ -z "$line" ]; then
			continue
		fi
		if [ ${line:0:1} == "#" ]; then
			continue
		fi
		query "$line" "$color"
		if [ $? != 0 ]; then
			break
		fi
	done
}

if [ -z "$file" ]; then
	exe
else
	cat "$file" | exe
fi
