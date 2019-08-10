#!/bin/bash

set -eu
set -o pipefail

source _env_load.sh

#echo "=> ./load.sh partsupp"
#./load.sh partsupp $blocks
echo "=> ./load.sh lineitem"
./load.sh lineitem $blocks
#echo "=> ./load.sh orders"
#./load.sh orders $blocks
#echo "=> ./load.sh customer"
#./load.sh customer $blocks
#echo "=> ./load.sh supplier"
#./load.sh supplier $blocks
#echo "=> ./load.sh part"
#./load.sh part $blocks
#echo "=> ./load.sh nation"
#./load.sh nation 1
#echo "=> ./load.sh region"
#./load.sh region 1

