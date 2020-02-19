#!/bin/bash

FLASH_HOME="$(dirname `cd $(dirname ${BASH_SOURCE[0]}) && pwd`)"

export PATH=$PATH:"$FLASH_HOME/integrated/ops/"

# call ci jenkins test
echo "start ci jenkins test"
ti.sh ci/jenkins

# call ci release test
echo "start ci release test"
ti.sh ci/release
