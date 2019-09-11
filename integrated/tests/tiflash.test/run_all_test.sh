#!/bin/bash

set -euo pipefail

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
integrated="$(dirname `dirname ${here}`)"

"${here}/run_test.sh" "${here}/sample.test" "${integrated}/ops/ti.sh" "${integrated}/ti/1+spark.ti"
"${here}/run_test.sh" "${here}/ddl_syntax" "${integrated}/ops/ti.sh" "${integrated}/ti/1+spark.ti"
