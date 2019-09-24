#!/bin/bash

./cluster_dsh.sh "./stop_rngine_and_tiflash.sh"
sleep 1
./cluster_dsh.sh "./stop_tidb.sh"
sleep 1
./cluster_dsh.sh "./stop_tikv.sh"
sleep 1
./cluster_dsh.sh "./stop_pd.sh"
sleep 1
./cluster_dsh.sh "./spark_stop_all.sh"
