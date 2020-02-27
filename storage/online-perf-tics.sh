#!/bin/bash

set -x

address="$1"
seconds="$2"
frequency="$3"
flame_graph="$4"
output_path="$5"

result=$(echo ${output_path} | grep "\.svg$")
if [[ -z ${result} ]]; then
    echo "output file should end with .svg"
    exit 1
fi

if [[ ${flame_graph} == "false" ]]; then
    # generate a svg file according to the protobuf.
    TMP_FILE=$(mktemp /tmp/tmp.XXXXXXXXXX) || exit 1
    curl -H 'Content-Type:application/protobuf' "http://${address}/debug/pprof/profile?seconds=${seconds}&frequency=${frequency}" > ${TMP_FILE}
    # should install graphviz first.
    go tool pprof -svg ${TMP_FILE} > ${output_path}
    rm ${TMP_FILE}
else
    curl "http://${address}/debug/pprof/profile?seconds=${seconds}&frequency=${frequency}" > ${output_path}
fi
