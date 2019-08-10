#!/bin/bash

source ./_env_build.sh

function check_pid()
{
	local file="$1"
	local name="$2"
	echo -e "\033[0m=> $file $name\033[0m"
	local pid=`get_pid "$file" "$name"`
	if [ -z "$pid" ]; then
		echo -e "   \033[35;1mnot running\033[0m"
	else
		echo -e "   \033[32;1m$pid\033[0m"
	fi
}
export -f check_pid

function check_log()
{
	local log="$1"
	local word="$2"
	local title="$3"
	local length="$4"
	local red="$5"

	if [ ! -f "$log" ]; then
		echo -e "\033[31m   not found: ${log}\033[0m"
	else
		local got=`tail -n 6666 "$log" | grep -v "failed to bind addr for status service" | grep -v "send response fail: RemoteStopped" | grep "$word" | tail -n 1 | awk '{$1="";print}'`
		if [ -z "$got" ]; then
			echo -e "   \033[32mno ${title}s\033[0m"
		else
			if [ ${length} -gt ${#got} ]; then
				length="${#got}"
			fi
			if [ "$red" == "true" ]; then
				echo -e "  \033[35m${got:0:$length}\033[0m"
			else
				echo -e "  \033[36m${got:0:$length}\033[0m"
			fi
		fi
	fi
}
export -f check_log

function show_load()
{
	local dev="$1"
	local load=`uptime | awk '{print $10}' | awk -F ',' '{print $1}'`
	echo "=> LOAD"
	local cores=`grep 'model name' /proc/cpuinfo | wc -l`
	echo -e "   \033[34;1m$load\033[0m load / $cores cores"
	local io=`iostat -m -d 1 2 | grep "$dev" | tail -n 1 | \
		awk '{print "\033[34;1m"$2"\033[0m/tps, \033[34;1m"$3"\033[0mm/r, \033[34;1m"$4"\033[0mm/w of "$1}'`
	echo -e "   $io"
}
export -f show_load

function show_top_regions()
{
	local n="$1"

	if [ -z "`get_pid pd-server 'name=pd1'`" ]; then
		return
	fi

	local info=`./$bin/pd-ctl -d -u=http://$ip:$pd_port region`
	local count=`echo "$info" | grep count | awk '{print $2}' | awk -F ',' '{print $1}'`
	local regions=`echo "$info" | python ./_parse_regions.py \
		| sort -rnk 2 | head -n $n`

	if [ -z "$regions" ]; then
		return
	fi

	echo -e "=> top $n regions by keys, \033[36m$count\033[0m total"
	local out=""
	while read region; do
		local region_id=`echo $region | awk '{print $1}'`
		local region_keys=`echo $region | awk '{print $2}'`
		out="$out(\033[36m${region_id}\033[0m:\033[36m${region_keys}\033[0m)"
	done <<< "$(echo -e "$regions")"
	echo -e "   ${out}"
}
export -f show_top_regions

function show_region_partition()
{
	local n="$1"
	if [ -z "`get_pid '$tiflash_bin_name server' flash`" ]; then
		return
	fi

	local map=`./$bin/$tiflash_bin_name client --host $ip --port $tiflash_tcp_port --query "DBGInvoke dump_partition()" \
		2>/dev/null | awk '{print "   "$0}'`
	if [ -z "$map" ]; then
		return
	fi
	local entrys="`echo "$map" | wc -l`"
	echo -e "=> top partitions, total \033[36m${entrys}\033[0m regions in mapping"
	echo -e "$map" | awk '{print $2, $4}' | sort | uniq -c | sort -nrk1 | \
		awk '{print "   \033[36m"$1"\033[0m regions in table "$2" partition "$3}' | head -n "$n"
}
export -f show_region_partition

function check_sync_status()
{
	local log="$1"

	if [ ! -f "$log" ]; then
		return
	else
		local out=`tail -n 10000 "$log" | grep "KVStore: Sync status" | tail -n 1`
		if [ ! -z "$out" ]; then
			local ts=`echo "$out" | awk '{print $2}' | awk -F '\\\.' '{print $1}'`
			local region=`echo "$out" | awk -F 'status: ' '{print $2}'`
			echo -e "   \033[36m$ts\033[0m(`date +%T`) applied $region"
		fi
	fi
}
export -f check_sync_status

function check_move_status()
{
	local log="$1"

	if [ ! -f "$log" ]; then
		return
	else
		local out=`tail -n 10000 "$log" | grep Moved | tail -n 1`
		if [ ! -z "$out" ]; then
			local ts=`echo "$out" | awk '{print $2}' | awk -F '\\\.' '{print $1}'`
			out=`echo "$out" | awk -F 'Moved ' '{print $2}' | \
				sed -r 's/9223372036854775808/inf/g'`
			echo -e "   \033[36m$ts\033[0m(`date +%T`) moved $out"
		fi
	fi
}
export -f check_move_status

function show_partitions()
{
	local server_path="$1"
	local table="$2"
	local db="$3"

	local got=`cd $server_path && analyze-table-compaction.sh "$table" "$db" 2>/dev/null | awk '{print "   "$0}'`
	local count=`echo "$got" | head -n 1 | awk '{print $NF}'`
	local parts=`echo "$got" | grep "Parts total" | awk '{print $NF}'`
	if [ -z "$parts" ]; then
		return
	fi
	echo -e "=> ${db}.${table} status"
	echo -e "   \033[36m$count\033[0m partitions, \033[36m$parts\033[0m parts"
        local size=`echo "$got" | grep "batch size" | awk '{print $(NF-2)}'`
        if [ "$size" != "batch" ]; then
                echo -e "   \033[36m"$size"\033[0mmb write batch"
        fi
	echo "$got" | grep "write amp" | awk '{print "   \033[36m"$NF"\033[0m write amplification"}'
}
export -f show_partitions
