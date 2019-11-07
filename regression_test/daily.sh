#!/bin/bash

FLASH_HOME="$(dirname `cd $(dirname ${BASH_SOURCE[0]}) && pwd`)"

export PATH=$PATH:"$FLASH_HOME/integrated/ops/"
cp "$FLASH_HOME/regression_test/conf/bin.paths" "$FLASH_HOME/integrated/conf/"

# simple test
ti.sh new ci.ti
ti.sh ci.ti 'up:tpch/load 0.1 all'
ti.sh ci.ti burn doit


# call ci release test
# loading tpch_0_01_tmt_no_raft.lineitem to CH directly
# environment: line 12: /tmp/ti/master/bins/tiflash: No such file or directory
# ti.sh ci/release

# call ci test
# loading tpch_0_01_tmt_no_raft.lineitem to CH directly
# environment: line 12: /tmp/ti/master/bins/tiflash: No such file or directory
# "$FLASH_HOME/integrated/tests/ci/jenkins.sh"

# call tpch test
# ERROR 9005 (HY000) at line 4: Region is unavailable
#"$FLASH_HOME/integrated/tests/performance/tpch_perf_10.sh"
