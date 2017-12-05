query="$1"
if [ -z "$query" ]; then
	query="SELECT * FROM test"
fi
DYLD_LIBRARY_PATH="" build/dbms/src/Magic/ch-raw "running/config/config.xml" "$query"
