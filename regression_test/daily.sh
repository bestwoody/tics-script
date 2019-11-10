#!/bin/bash

FLASH_HOME="$(dirname `cd $(dirname ${BASH_SOURCE[0]}) && pwd`)"

export PATH=$PATH:"$FLASH_HOME/integrated/ops/"

# simple test
echo "start simple test"
rm -rf simple.ti
ti.sh new simple.ti
ti.sh simple.ti 'up:sleep 5:tpch/load 0.1 all:burn doit'
rm -rf simple.ti

# FLASH-656
echo "start FLASH-656 test"
rm -rf ci-656.ti
ti.sh new ci-656.ti
ti.sh ci-656.ti 'up:sleep 5:syncing/test:burn doit'
rm -rf ci-656.ti

# call tpch test
echo "start tpch test"
rm -rf tpch.ti
ti.sh new tpch.ti
ti.sh tpch.ti 'up:sleep 5:tpch/load 10 all:tpch/ch:burn doit'
rm -rf tpch.ti

# call ci jenkins test
echo "start ci jenkins test"
ti.sh ci/jenkins

# call ci release test
echo "start ci release test"
ti.sh ci/release

