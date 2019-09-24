#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

rm -rf ./${data}
rm -rf ./${log}