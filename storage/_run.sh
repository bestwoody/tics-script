#!/bin/bash

function echo_hl()
{
	local line="$1"
	local color="$2"
	if [ "$color" == "true" ]; then
		echo -e "\033[32m$line\033[0m"
	else
		echo "$line"
	fi
}

function dcall()
{
	local cmd="$1"
	local color="$2"
	echo_hl "=> DBGInvoke $cmd" "$color"
	./storage-client.sh "DBGInvoke $cmd" -f PrettyCompactNoEscapes
}
export dcall

function query()
{
	local q="$1"
	local color="$2"
	echo_hl "=> $q" "$color"
	./storage-client.sh "$q" -f PrettyCompactNoEscapes
}
export query
