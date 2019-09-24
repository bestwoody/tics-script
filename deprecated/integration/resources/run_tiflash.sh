#!/bin/bash

deamon="$1"
run_rngine="$2"

set -eu
set -o pipefail

source ./_env_build.sh

exec $bin/${tiflash_bin_name} server --config-file "$config/config.xml" >$log/theflash.log 2>$log/theflash_stderr.log &
