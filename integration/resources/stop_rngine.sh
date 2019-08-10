#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

echo "=> stop tikv-server-rngine"
stop ${tiflash_proxy_bin_name} rngine
sleep 1