#!/bin/bash

set -euo pipefail

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
integrated="$(dirname `dirname ${here}`)"

"${here}/run_test.sh" "${here}/sample.test" "${integrated}/ops/ti.sh" "${integrated}/ti/cluster/1+spark.ti"
"${here}/run_test.sh" "${here}/ddl_syntax" "${integrated}/ops/ti.sh" "${integrated}/ti/cluster/1+spark.ti"