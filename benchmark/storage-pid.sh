ps -ef | grep 'theflash server' | grep -v grep | awk '{print $2}'
