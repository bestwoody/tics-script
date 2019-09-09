#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

echo '=> pd:'
ls_pd_proc
echo '=> tikv:'
ls_tikv_proc
echo '=> tidb:'
ls_tidb_proc
echo '=> tiflash:'
ls_tiflash_proc
echo '=> rngine:'
ls_rngine_proc

# TODO: list Spark proc
