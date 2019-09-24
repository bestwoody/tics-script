#!/bin/bash

set -eu

source ./_meta.sh

get_table_names | while read table; do
	./dbgen.sh "$table"
done
