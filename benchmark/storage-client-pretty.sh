#!/bin/bash

query="$1"
server="$2"
if [ -z "$host" ]; then
	server="127.0.0.1"
fi

./storage-client.sh "$query" "$server" PrettyCompactNoEscapes
