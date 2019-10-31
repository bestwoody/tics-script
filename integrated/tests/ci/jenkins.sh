#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

cd $integrated

ops/ti.sh help
ops/ti.sh example
ops/ti.sh new my.ti
ops/ti.sh procs

ops/ti.sh tests/ci/ci.ti burn doit
ops/ti.sh tests/ci/ci.ti run
ops/ti.sh tests/ci/ci.ti status
ops/ti.sh tests/ci/ci.ti prop
ops/ti.sh tests/ci/ci.ti stop:run:status
ops/ti.sh tests/ci/ci.ti mysql "show databases"
ops/ti.sh tests/ci/ci.ti tpch/load 0.01 all
ops/ti.sh tests/ci/ci.ti mysql "show databases"
ops/ti.sh tests/ci/ci.ti mysql "select count(*) from tpch_0_01.customer"
ops/ti.sh tests/ci/ci.ti burn doit
