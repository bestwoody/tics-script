#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

echo "=> stop tidb"
stop tidb-server tidb true
sleep 1