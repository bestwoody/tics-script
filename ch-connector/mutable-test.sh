dbc="build/dbms/src/Server/clickhouse client -f PrettyCompactNoEscapes --query"

for file in mutable-test/*; do
	if [ ! -f "$file" ]; then
		continue
	fi
	python mutable-test.py "$dbc" "$file"
	if [ $? == 0 ]; then
		echo $file: OK
	else
		echo $file: Failed
		exit 1
	fi
done
