#!/bin/bash

set -x
set -euo pipefail

FLASH_HOME="$(dirname `cd $(dirname ${BASH_SOURCE[0]}) && pwd`)"

export PATH=$PATH:"$FLASH_HOME/integrated/ops/"

ti.sh ci/jenkins
ti.sh ci/release
ti.sh ci/tpch_diff
ti.sh ci/tidb_test
ti.sh ci/copr-test
