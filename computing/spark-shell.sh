#!/bin/bash

cp conf/spark-defaults.conf spark/conf/
if [ "$?" != "0" ]; then
	echo "Copy config file to spark failed." >&2
	exit 1
fi

spark/bin/spark-shell $@
