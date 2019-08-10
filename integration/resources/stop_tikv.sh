#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

echo "=> stop tikv-server"
stop tikv-server ${kv_name}
sleep 1
