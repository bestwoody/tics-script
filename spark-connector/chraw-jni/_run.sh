arg1="$1"
arg2="$2"
arg3="$3"
arg4="$4"

set -eu

cd target

lib_path="../../../ch-connector/build/dbms/src/TheFlash"
lib="$lib_path/libch.dylib"
if [ -f "$lib" ]; then
	cp $lib .
fi
lib="$lib_path/libch.so"
if [ -f "$lib" ]; then
	cp $lib .
fi

java -cp TheFlashProtoBench-1.0.jar:lib/* pingcap.com/App "$arg1" "$arg2" "$arg3" "$arg4" 2>&1 | grep -v 'SLF4J: '
