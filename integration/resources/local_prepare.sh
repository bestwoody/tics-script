#!/bin/bash

set -eu

source ./_env_build.sh

./download_bin.sh
./spark_download.sh
mv ./spark-defaults.conf spark/conf/spark-defaults.conf
mv "${bin}tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar" "spark/jars/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
./update_tiflash_from_h59.sh
./build_rngine_config.sh
./build_tiflash_config.sh
