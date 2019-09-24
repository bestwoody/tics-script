#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

rm -rf ./${data}/theflash
rm -rf ./${data}/rngine