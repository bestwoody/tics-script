set -ue

source ./_env.sh

log_path=`grep '<log>' "$storage_server_config" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
if [ -f "$log_path" ]; then
tail -n 10000 $log_path | grep persisted | awk -F 'Global ' '{print $2}' | tail -n 1 | grep --color 'persisted cache'
fi

cache_path=`grep '<persisted_mapping_cache_path>' "$storage_server_config" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
if [ ! -z "$cache_path" ]; then
	cache_paths=(`echo "$cache_path" | tr ';' ' '`)
	echo
	for path in ${cache_paths[@]}; do
		if [ -d "$path" ]; then
			du -sh "$path"
		fi
	done
else
	echo "persisted cache disabled"
fi
