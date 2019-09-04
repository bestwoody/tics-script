#!/bin/bash
integrated="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"

${integrated}/tests/run_test.sh ${integrated}/tests/sample.test ${integrated}/ops/ti.sh ${integrated}/ti/cluster/1+spark.ti
${integrated}/tests/run_test.sh ${integrated}/tests/ddl_syntax ${integrated}/ops/ti.sh ${integrated}/ti/cluster/1+spark.ti
