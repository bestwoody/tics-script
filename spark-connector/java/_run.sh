args="$@"

set -eu

cd target

lib_path="../../../ch-connector/build/dbms/src/Magic"
lib="$lib_path/libch.dylib"
if [ -f "$lib" ]; then
	cp $lib .
fi
lib="$lib_path/libch.so"
if [ -f "$lib" ]; then
	cp $lib .
fi

java -cp MagicProtoBench-1.0.jar:lib/* pingcap.com/App $args 2>&1 | grep -v 'SLF4J: '
