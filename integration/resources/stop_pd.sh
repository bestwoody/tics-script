#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

echo "=> stop pd-server"
stop pd-server "name=$pd_name"
sleep 1