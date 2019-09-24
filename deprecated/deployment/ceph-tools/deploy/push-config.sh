#!/bin/bash

set -eu
source ./_env.sh
ceph-deploy --username "$user" --overwrite-conf config push ${nodes[@]}
