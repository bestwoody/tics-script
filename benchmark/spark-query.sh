source ./conf/dirconf
source _env.sh

if [ -z "$@" ]; then
        echo "<bin> usage: sql query "
        exit 1
fi
cat $envDir > $runingDir
./ch-client.sh "show tables" | while read tableName;
do
        echo "ch.mapCHClusterTable(table=\"$tableName\")" >> $runingDir
done
echo "val startTime = new Date()">> $runingDir
echo "ch.sql(\"$@\").show" >> $runingDir
cat $endDir >> $runingDir
./spark-shell.sh < $runingDir
rm -rf $runingDir
