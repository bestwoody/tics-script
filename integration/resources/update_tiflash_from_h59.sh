#!/bin/bash

scp root@172.16.5.59:/data1/poc-compile/tiflash/storage/build/dbms/src/Server/theflash ${bin}/theflash.new

if [ -f "$bin/${tiflash_bin_name}" ]; then
	mv -f $bin/${tiflash_bin_name} $bin/theflash.old
fi

mv $bin/theflash.new $bin/${tiflash_bin_name}
