#!/bin/bash

set -u

echo "=> ./daemon-start-mon.sh"
./daemon-start-mon.sh
sleep 5
sudo ceph -s
sleep 2

echo "=> ./daemon-start-mgr.sh"
./daemon-start-mgr.sh
sleep 5
sudo ceph -s
sleep 2

echo "=> ./daemon-start-osd.sh"
./daemon-start-osd.sh
sleep 5
sudo ceph -s
sleep 2

echo "=> ./daemon-start-mds.sh"
./daemon-start-mds.sh
sleep 5
sudo ceph -s
