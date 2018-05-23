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
	echo "=> building arrow:"
	./arrow-build-linux.sh
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
mkdir -p "$name/inventory/scripts"
cp -f ./scripts/spark-*.sh "./$name/inventory/scripts"
cp -f ./scripts/storage-*.sh "./$name/inventory/scripts"
cp -f ./scripts/_*.sh "./$name/inventory/scripts"

if [ "$build" == "true" ]; then
	cd "../spark-connector"
	cd "spark"
	echo "=> building spark:"
	mvn -DskipTests clean package
	cd ".."
	echo "=> building chspark:"
	./build-all.sh
	cd "../publish"
fi

echo "=> copying spark (with chspark)"
mkdir -p "$name/spark"
cp -rf "../spark-connector/spark/sbin" "./$name/spark/"
cp -rf "../spark-connector/spark/bin" "./$name/spark/"
cp -rf "../spark-connector/spark/conf" "./$name/spark/"
cp -rf "../spark-connector/spark/assembly" "./$name/spark/"
cp -rf "../spark-connector/conf/spark-defaults.conf" "./$name/spark/conf/"

echo "=> copying tpch dbgen"
if [ "$build" == "true" ]; then
	cd "../benchmark/tpch-dbgen"
	make
	cd "../../publish"
fi
mkdir -p "$name/tpch"
cp -f "../benchmark/tpch-dbgen/dbgen" "./$name/tpch/"
cp -f "../benchmark/tpch-dbgen/dists.dss" "./$name/tpch/"

echo "=> copying tpch data loader and env"
cp -rf "../benchmark/sql-spark" "./$name/tpch/sql"
cp -rf "../benchmark/loading" "./$name/tpch/"
cp -f ./scripts/tpch-env.sh "./$name/tpch/loading/_env.sh"

cp "./unpack-dev-linux.sh" "./$name/unpack-inventory.sh"

echo "=> packing to ./${name}.tar.gz (may take some time)"
#tar -cvzf "./${name}.tar.gz" "$name"

echo "=> done"
