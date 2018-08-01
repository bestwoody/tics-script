#!/bin/bash

query="$1"

./storage-client.sh "$query" PrettyCompactNoEscapes
