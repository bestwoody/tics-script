#!/bin/bash

set -eu

source ./_env_load.sh

for table in lineitem part partsupp orders supplier customer;
do
echo $table
if [[ $blocks -gt 1 ]]; then
    if [[ ! -f ${data_src}/tpch${scale}_${blocks}/$table.tbl.1 ]]; then
        ./dbgen.sh $table $blocks
        if [[ ! -f ${data_src}/tpch${scale}_${blocks} ]]; then
            mkdir -p ${data_src}/tpch${scale}_${blocks}
        fi
        mv bin/$table.* ${data_src}/tpch${scale}_${blocks}
    fi
else
    if [[ ! -f ${data_src}/tpch${scale}/$table.tbl ]]; then
        ./dbgen.sh $table $blocks
        if [[ ! -f ${data_src}/tpch${scale} ]]; then
            mkdir -p ${data_src}/tpch${scale}
        fi
        mv bin/$table.* ${data_src}/tpch${scale}
    fi
fi
done;

for table in region nation;
do
if [[ ! -f ${data_src}/tpch${scale}/$table.tbl ]]; then
    ./dbgen.sh $table 1
    if [[ ! -f ${data_src}/tpch${scale} ]]; then
        mkdir -p ${data_src}/tpch${scale}
    fi
    mv bin/$table.* ${data_src}/tpch${scale}
fi
done;
