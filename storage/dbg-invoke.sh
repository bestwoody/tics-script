#!/bin/bash

set -eu
source ./_env.sh

"$storage_bin" client --host "$storage_server" -d "$storage_db" --query "DBGInvoke $@"
