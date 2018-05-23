build="$1"
name="$2"

set -eu

if [ -z "$build" ]; then
	echo "usage: <bin> build-before-publish(true|false) [output-name]" >&2
	exit 1
fi

if [ -z "$name" ]; then
	name="theflash-`date +%s`"
fi

echo "=> packing publish package $name"

if [ "$build" == "true" ]; then
	cd "../ch-connector"
	echo "=> building theflash:"
	./build.sh
	cd "../publish"
fi

mkdir -p "$name/inventory"

echo "=> copying theflash"
cp -f "../ch-connector/build/dbms/src/Server/theflash" "./$name/inventory"
cp -f "../ch-connector/running/config/config.xml" "./$name/inventory"
cp -f "../ch-connector/running/config/users.xml" "./$name/inventory"

echo "=> copying libs"
mkdir -p "$name/inventory/dylibs"
ldd ../ch-connector/build/dbms/src/Server/theflash | grep '/' | grep '=>' | awk -F '=>' '{print $2}' | awk '{print $1}' | while read libfile; do
	cp -f "$libfile" "./$name/inventory/dylibs"
done

echo "=> copying scripts"
cp -f "../benchmark/_env.sh" "./$name/inventory"
cp -f "../benchmark/_helper.sh" "./$name/inventory"

if [ "$build" == "true" ]; then
	cd "../spark-connector"
	echo "=> building chspark:"
	./build-all.sh
	cd "../publish"
fi

echo "=> copying chspark"
cp -f "../spark-connector/chspark/target/chspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar" "./$name/inventory"

cp "./unpack-dev-linux.sh" "./$name/unpack.sh"

echo "=> packing to ./${name}.tar.gz"
tar -cvzf "./${name}.tar.gz" "$name"

echo "=> done"
