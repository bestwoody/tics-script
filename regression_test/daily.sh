#!/bin/bash

set -x

FLASH_HOME="$(dirname `cd $(dirname ${BASH_SOURCE[0]}) && pwd`)"

export PATH=$PATH:"$FLASH_HOME/integrated/ops/"

ti.sh ci/jenkins
ti.sh ci/release
