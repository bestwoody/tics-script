grep -i 'Error\|Warn' /data/ch-server/server.log \
	| grep -v 'DB::NetException'
