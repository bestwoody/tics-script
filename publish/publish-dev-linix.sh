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

echo "=> building $name"

if [ "$build" == "true" ]; then
	cd "../ch-connector"
	echo "=> building arrow:"
	./arrow-build-linux.sh
	echo "=> building theflash:"
	./build.sh
	cd "../publish"
fi

mkdir -p "$name"

echo "=> copying theflash:"
cp -f "../ch-connector/build/dbms/src/Server/theflash" "./$name"
cp -f "../ch-connector/running/config/config.xml" "./$name"
cp -f "../ch-connector/running/config/users.xml" "./$name"

echo "=> copying lib:"
ldd ../ch-connector/build/dbms/src/Server/theflash | grep '/' | grep '=>' | awk -F '=>' '{print $2}' | awk '{print $1}' | while read libfile; do
	cp -f "$libfile" "./$name"
done

if [ "$build" == "true" ]; then
	cd "../spark-connector"
	echo "=> building chspark:"
	./build-all.sh
	cd "../publish"
fi

echo "=> copying chspark:"
cp -f "../spark-connector/chspark/target/chspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar" "./$name"

echo "=> packing to ./${name}.tar.gz"
tar -cvzf "./${name}.tar.gz" "$name"

echo "=> done"
