args="$@"

set -eu

cd target

lib_path="../../cpp/build"
lib="$lib_path/libbench.dylib"
if [ -f "$lib" ]; then
	cp $lib .
fi
lib="$lib_path/libbench.so"
if [ -f "$lib" ]; then
	cp $lib .
fi

java -cp TheFlashProtoBench-1.0.jar:lib/* pingcap.com/App $args 2>&1 | grep -v 'SLF4J: '
