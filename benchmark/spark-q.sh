source ./conf/dirconf
source _env.sh
n=$1

if [ -z "$n" ]; then
        echo "usage: <bin> n"
        exit 1
fi

cat $envDir > $runingDir
./ch-client.sh "show tables" | while read tableName;
do
	echo "ch.mapCHClusterTable(table=\"$tableName\")" >> $runingDir
done
echo "val startTime = new Date()">> $runingDir

sql=`cat sql-spark/q"$n".sql | tr '\n' ' '`
echo "ch.sql(\"$sql\").show" >> $runingDir
cat $endDir >> $runingDir
./spark-shell.sh < $runingDir
rm -rf $runingDir
