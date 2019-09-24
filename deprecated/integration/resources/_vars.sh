#!/bin/bash

export RUST_BACKTRACE=1
export RUST_LOG=debug

export TZ=${TZ:-/etc/localtime}
#export TZ=Asia/Shanghai
export GRPC_VERBOSITY=DEBUG
#export GRPC_TRACE=all

#ulimit -n 1000000
