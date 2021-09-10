db_name="randgen_"$test_goal
tidb_port=4002
tidb_ip="172.16.5.85"
output_file=""
test_data=""
pre_mysql="ti_mysql> "
pre_func="ti_func> "
pre_tiflash="ti_mysql> tiflash "
sql_mode="set @@global.sql_mode ='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"
cmd="set @@tidb_isolation_read_engines='tiflash,tidb'; set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;"

function test_a_file() {
    total_queries=0
    touch tmp
    touch $output_file
    create_table=""
    table_name=""
    db_name=""
    use=""
    tbls=()
    insert=""
    lineID=0
    query_num=0
    while read -r line; do
        lineID=$((lineID + 1))
        if [[ $line == SELECT* ]]; then
        	if [[ $total_queries == 0 ]]; then
        		for tbl in "${tbls[@]}"
				do
					echo ${pre_func}"wait_table "${db_name}" "${tbl} >> $output_file
				done
        	fi
        	total_queries=$((total_queries + 1))
        	res=$(mysql --silent -h $tidb_ip -P $tidb_port -u root -f -D $db_name -BNe "${use}${cmd}$line" 2>&1)
        	if [ $? -eq "0" ]; then
        		count=${#res[@]}
        		if [[ ${count} -lt 1000 ]]; then
					echo "${pre_tiflash}${use}${cmd}${line}" >> $output_file
					echo "${use}${cmd}$line" | mysql -t -h $tidb_ip -P $tidb_port -u root -f -D $db_name >> $output_file 2>&1
					echo "" >> $output_file
					query_num=$((query_num + 1))
				else
					echo "${lineID} query is IGNORED due to too many rows [${count}]."
				fi
			else
				echo "${lineID} query $res"
            fi
            if [[ $((${total_queries}%400)) -eq 0 ]]; then
            	echo "${total_queries} queries have been processed."
            fi
        elif [[ $line == CREATE\ TABLE* ]]; then
			create_table=${create_table}" "${line}
			table_name=${line#*"\`"}
			table_name=${table_name%"\`"*}
			tbls+=("$table_name")
        elif [[ $line == CREATE\ DATABASE* ]]; then
			db_name=${line#*"\`"}
			db_name=${db_name%"\`"*}
			echo ${pre_mysql}"drop database if exists "$db_name";" >> $output_file
        	echo ${pre_mysql}$line >> $output_file
        	use="use $db_name; "
        elif [[ $line == \)\ * ]]; then
            create_table=${create_table}" "${line}
            echo ${pre_mysql}${use}${create_table} >> $output_file
            echo ${pre_mysql}"alter table "${db_name}"."${table_name}" set tiflash replica 1;" >> $output_file
            create_table=""
        elif [[ $line == UNLOCK\ TABLES* ]]; then
        	echo ${pre_mysql}${use}${insert} >> $output_file
        	echo ${pre_mysql}${use}$line >> $output_file
        	insert=""
        	#echo ${pre_func}"wait_table "${db_name}" "${table_name} >> $output_file
        elif [[ $line == INSERT* ]]; then
        	if [[ $((${#insert} + ${#line})) -gt 80000 ]]; then
        		echo ${pre_mysql}${use}${insert} >> $output_file
        		insert=${line}
        	else
        		insert=${insert}" "${line}
        	fi
        else
            if [[ ${create_table} != "" ]]; then
            	create_table=${create_table}" "${line}
            elif [[ $line != /* ]] && [[ $line != "" ]] && [[ $line != \#* ]] && [[ $line != --* ]] && [[ $line != USE* ]]; then
            	echo ${pre_mysql}${use}$line >> $output_file
    		fi
        fi
    done < $test_data
    echo "">> $output_file
    echo ${pre_mysql}"drop database if exists "$db_name";" >> $output_file
    sed -i "s/\`//g" $output_file
    echo "$test_data is DONE, ${query_num} queries of total ${total_queries} are recored!!"
}

if [ $# -eq 4 ]; then
    ## input: tidb_ip tidb_port randgen_file output_file
    ## run mpp for test files in $dir
    tidb_ip=$1
    tidb_port=$2
    test_data=$3
    output_file=$4
    echo "# ti.sh my.ti ci/fullstack randgen-mpp/$output_file false" > $output_file
    echo "${pre_mysql}$sql_mode" >> $output_file
    test_a_file
else
	echo "please input tidb_ip tidb_port randgen_file output_file"
fi
