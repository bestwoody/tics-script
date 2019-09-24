#!/bin/bash

sudo bash -c "echo 1 > /proc/sys/vm/drop_caches && sysctl -p 1>/dev/null 2>&1"
