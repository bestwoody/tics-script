#!/bin/bash

set -x
set -euo pipefail

FLASH_HOME="$(dirname `cd $(dirname ${BASH_SOURCE[0]}) && pwd`)"

export PATH=$PATH:"$FLASH_HOME/integrated/ops/"

ti.sh $@
