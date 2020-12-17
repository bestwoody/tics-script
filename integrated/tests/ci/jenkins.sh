#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

cd "${integrated}"

## Download the latest binaries
echo "Downloading binaries ... "
BIN_PATHS_CFG_FILE="${integrated}/conf/bin.paths"
BIN_PATHS_CFG_FILE_SAVED="${BIN_PATHS_CFG_FILE}.saved"

if [ -f "${BIN_PATHS_CFG_FILE}" ]; then
    mv "${BIN_PATHS_CFG_FILE}" "${BIN_PATHS_CFG_FILE_SAVED}"
fi
cp "${integrated}/tests/ci/conf/bin.paths" "${BIN_PATHS_CFG_FILE}"

ops/ti.sh download "${integrated}/tests/ci/conf/download.ti" "${integrated}/binary"
ops/ti.sh "${integrated}/tests/ci/conf/download.ti" burn : up : ver : burn

echo "Running tests ... "
ops/ti.sh ci/jenkins

# Restore config file
if [ -f "${BIN_PATHS_CFG_FILE_SAVED}" ]; then
    mv "${BIN_PATHS_CFG_FILE_SAVED}" "${BIN_PATHS_CFG_FILE}"
fi
