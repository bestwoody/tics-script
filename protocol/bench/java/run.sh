args="$@"

set -eu

cd target

lib=../../cpp/build/libbench.dylib
if [ -f "$lib" ]; then
	cp $lib .
fi
lib=../../cpp/build/libbench.so
if [ -f "$lib" ]; then
	cp $lib .
fi

java -cp MagicProtoBench-1.0.jar:lib/* pingcap.com/App $args
