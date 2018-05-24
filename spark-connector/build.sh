set -eu

old=`pwd`

cd chspark
mvn clean package
cd "$old"

spark_jp=""
for jp in spark/assembly/target/scala-*/jars; do
	spark_jp="$jp"
	break
done
if [ ! -d "$spark_jp" ]; then
	echo "spark jars path not found" >&2
	exit 1
fi
cp "chspark/target/chspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar" "$spark_jp/"

echo
echo "OK"
