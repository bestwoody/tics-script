#!/bin/bash

set -euo pipefail

"${integrated}/ops/ti.sh" clean_env /tmp/ti/ci
"${integrated}/ops/ti.sh" ci/self
"${integrated}/ops/ti.sh" ci/cluster
"${integrated}/ops/ti.sh" ci/fullstack
"${integrated}/ops/ti.sh" ci/tidb_snapshot
"${integrated}/ops/ti.sh" ci/tidb_priv
