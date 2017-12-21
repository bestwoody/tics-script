source ./conf/dirconf
n=$1

if [ -z "$n" ]; then
        echo "usage: <bin> n"
        exit 1
fi

cat $headerDir > $runingDir$n
./ch-client.sh "show tables" | while read tableName;
do
	echo "ch.mapCHClusterTable(table=\"$tableName\")" >> $runingDir$n
done
echo "val startTime = new Date()">> $runingDir$n

sql=`cat sql-spark/q"$n".sql | tr '\n' ' '`
echo "ch.sql(\"$sql\").show" >> $runingDir$n
cat $footerDir >> $runingDir$n
./spark-shell.sh < $runingDir$n
rm -rf $runingDir$n
