#!/bin/bash

source "./_env_build.sh"

cat<<EOF>./$config/rngine.toml
# TiKV config template
#  Human-readable big numbers:
#   File size(based on byte): KB, MB, GB, TB, PB
#    e.g.: 1_048_576 = "1MB"
#   Time(based on ms): ms, s, m, h
#    e.g.: 78_000 = "1.3m"


[readpool.storage]

[readpool.coprocessor]

[server]
labels = { zone = "engine" }
engine-addr = "127.0.0.1:9830"

[storage]

[pd]
# This section will be overwritten by command line parameters

[metric]
#address = "${ip}:9531"
#interval = "15s"
#job = "tikv"

[raftstore]
raftdb-path = ""
sync-log = true
#max-leader-missing-duration = "22s"
#abnormal-leader-missing-duration = "21s"
#peer-stale-state-check-interval = "20s"

[coprocessor]

[rocksdb]
wal-dir = ""

[rocksdb.defaultcf]
block-cache-size = "10GB"

[rocksdb.lockcf]
block-cache-size = "4GB"

[rocksdb.writecf]
block-cache-size = "4GB"

[raftdb]

[raftdb.defaultcf]
block-cache-size = "1GB"

[security]
ca-path = ""
cert-path = ""
key-path = ""

[import]

EOF