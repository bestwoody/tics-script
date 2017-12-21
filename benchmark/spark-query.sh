source ./conf/dirconf

if [ -z "$@" ]; then
        echo "<bin> usage: sql query "
        exit 1
fi
cat $headerDir > $runingDir
./ch-client.sh "show tables" | while read tableName;
do
        echo "ch.mapCHClusterTable(table=\"$tableName\")" >> $runingDir
done
echo "val startTime = new Date()">> $runingDir
echo "ch.sql(\"$@\").show" >> $runingDir
cat $footerDir >> $runingDir
./spark-shell.sh < $runingDir
rm -rf $runingDir
