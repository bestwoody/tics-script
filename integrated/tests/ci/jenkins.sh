#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

cd "${integrated}"

ops/ti.sh ci/jenkins
