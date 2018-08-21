set -ue

source ./_env.sh

log_path=`grep '<log>' "$storage_server_config" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
./storages-dsh.sh "tail -n 10000 $log_path | grep persisted | awk -F 'Global ' '{print \$2}' | tail -n 1" | grep --color 'persisted cache'

cache_path=`grep '<persisted_mapping_cache_path>' "$storage_server_config" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
if [ ! -z "$cache_path" ]; then
	echo
	./storages-dsh.sh "du -sh $cache_path"
fi
