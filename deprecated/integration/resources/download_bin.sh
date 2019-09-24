#!/bin/bash

source ./_env_build.sh

pd_server_url="http://139.219.11.38:8000/KGE5Z/pd-server"
pd_ctl_url="http://139.219.11.38:8000/DMTuq/pd-ctl"
tikv_server_url="http://139.219.11.38:8000/hfB3m/tikv-server"
tikv_ctl_url="http://139.219.11.38:8000/9zJSZ/tikv-ctl"
tidb_server_url="http://139.219.11.38:8000/jxmC4/tidb-server"
tiflash_proxy_url="http://139.219.11.38:8000/G0wkp/tikv-server-rngine"
chspark_url="http://139.219.11.38:8000/ga4sD/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar"

function download_bin_to_target_if_not_exists()
{
	local url="$1"
	local bin_name="$2"
	local target="$3"
	if [ ! -f "$target" ]; then
		echo "$target not exists."
		wget $url
		chmod a+x ./$bin_name
		mv ./$bin_name $target
	fi
}

download_bin_to_target_if_not_exists $pd_server_url pd-server "$bin/pd-server"
download_bin_to_target_if_not_exists $pd_ctl_url pd-ctl "$bin/pd-ctl"
download_bin_to_target_if_not_exists $tikv_server_url tikv-server "$bin/tikv-server"
download_bin_to_target_if_not_exists $tikv_ctl_url tikv-ctl "$bin/tikv-ctl"
download_bin_to_target_if_not_exists $tidb_server_url tidb-server "$bin/tidb-server"
download_bin_to_target_if_not_exists $tiflash_proxy_url tikv-server-rngine "$bin/$tiflash_proxy_bin_name"
download_bin_to_target_if_not_exists $chspark_url tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar "$bin/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
