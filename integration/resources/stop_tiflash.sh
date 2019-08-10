#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

echo "=> stop theflash"
stop ${tiflash_bin_name} flash
sleep 1