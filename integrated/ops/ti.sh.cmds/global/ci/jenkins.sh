#!/bin/bash

set -euo pipefail

"${integrated}/ops/ti.sh" ci/self
"${integrated}/ops/ti.sh" ci/cluster
